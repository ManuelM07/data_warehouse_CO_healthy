from conection import execute_query, insert_data
from dimension import Dimension
from pyspark.sql import SparkSession
from dotenv import dotenv_values
import pandas as pd

config = dotenv_values(".env")

spark = SparkSession.builder \
    .appName('Transformx') \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

PROCESS = "facturacion"
URL = config['URL_PAYMENT']
KEY = config['KEY_PAYMENT']
url_ = ""
option = {}

def get_dimensions():
    """Get all dimensions to use"""

    ob_dimension = Dimension()
    df_medical_center = ob_dimension.dim_medical_center()
    df_disease = ob_dimension.dim_disease()
    df_contributor= ob_dimension.dim_contributor()
    df_demographic = ob_dimension.dim_demographic()
    df_company = ob_dimension.dim_company()
    df_date = ob_dimension.dim_date("2006-01-01", "2010-12-31")

    return (df_medical_center, df_disease, df_contributor, 
            df_demographic, df_company, df_date)


def query_dimensions_demografic():
    global url_, options

    # Configuring the connection properties to CockroachDB
    url_ = config['JDBC_PAYMENT']
    options = {
        "user": config['USER_OUT'],
        "password": config['PASSWORD_OUT'],
        "driver": "org.postgresql.Driver"
    }

    dim_demographic = spark.read \
        .format("jdbc") \
        .option("url", url_) \
        .option("dbtable", "dim_demografica") \
        .options(**options) \
        .load()

    return dim_demographic


def insert_data_dim(df_contributor):
    """Insert the tables in the db"""

    (df_medical_center, df_disease, _,
     _, df_company, df_date) = get_dimensions()

    df_contributor = df_contributor.drop("proviene_otra_eps")
    df_disease = df_disease.withColumnRenamed("enfermedad", "nombre")
    df_contributor = df_contributor.toPandas()

    # Change type date to str
    df_contributor['fecha_nacimiento'] = pd.to_datetime(df_contributor['fecha_nacimiento'])
    df_contributor['fecha_nacimiento'] = df_contributor['fecha_nacimiento'].dt.strftime("%Y-%m-%d")
    df_contributor['fecha_afiliacion'] = pd.to_datetime(df_contributor['fecha_afiliacion'])
    df_contributor['fecha_afiliacion'] = df_contributor['fecha_afiliacion'].dt.strftime("%Y-%m-%d")

    insert_data(df_medical_center.toPandas(), "dim_centro_medico", URL, KEY)
    insert_data(df_disease.toPandas(), "dim_enfermedad", URL, KEY)
    insert_data(df_contributor, "dim_cotizante", URL, KEY)
    insert_data(df_date.toPandas(), "dim_fecha", URL, KEY)
    insert_data(df_company.toPandas(), "dim_empresa", URL, KEY)


def query_dimensions():
    global url_, options

    # Configuring the connection properties to CockroachDB
    dim_contributor = spark.read \
        .format("jdbc") \
        .option("url", url_) \
        .option("dbtable", "dim_cotizante") \
        .options(**options) \
        .load()
    
    dim_medical_center = spark.read \
        .format("jdbc") \
        .option("url", url_) \
        .option("dbtable", "dim_centro_medico") \
        .options(**options) \
        .load()
    
    dim_company = spark.read \
        .format("jdbc") \
        .option("url", url_) \
        .option("dbtable", "dim_empresa") \
        .options(**options) \
        .load()
    
    dim_desease = spark.read \
        .format("jdbc") \
        .option("url", url_) \
        .option("dbtable", "dim_enfermedad") \
        .options(**options) \
        .load()
    
    dim_date = spark.read \
        .format("jdbc") \
        .option("url", url_) \
        .option("dbtable", "dim_fecha") \
        .options(**options) \
        .load()

    return dim_contributor, dim_medical_center, dim_company, dim_desease, dim_date


def run():
    # Init ETL

    result, column_names = execute_query("SELECT * FROM pagos")
    df_payment = spark.createDataFrame(result, column_names)

    (_, _, df_contributor,
     df_demographic, _, _) =  get_dimensions()

    result, column_names = execute_query("SELECT * FROM COTIZANTE")
    df_contributor_aux = spark.createDataFrame(result, column_names)

    insert_data(df_demographic.toPandas(), "dim_demografica", URL, KEY)
    dim_demographic = query_dimensions_demografic()

    df_contributor = dim_demographic.join(df_contributor_aux, on=["direccion", "estado_civil", "estracto", "tipo_discapacidad", "salario_base"])
    df_contributor = df_contributor.select("cedula", "nombre", "tipo_cotizante", "sexo", "fecha_nacimiento", 
                                       "nivel_escolaridad", "fecha_afiliacion", "demografica_id")

    insert_data_dim(df_contributor)

    result, column_names = execute_query("SELECT * FROM EMPRESA_COTIZANTE")
    df_company_contributor = spark.createDataFrame(result, column_names)

    result, column_names = execute_query("SELECT * FROM PREEXISTENCIAS")
    df_preexistence = spark.createDataFrame(result, column_names)

    # Combine payment, company & preexistence
    merged_df_aux = df_payment.join(df_company_contributor, df_payment["id_usuario"]==df_company_contributor["cotizante"], "left")
    merged_df_aux = merged_df_aux.join(df_preexistence, "id_usuario", "left")
    merged_df_aux = merged_df_aux.join(df_contributor_aux.select("cedula", "id_ips"), merged_df_aux["id_usuario"]==df_contributor_aux["cedula"], "left")

    dim_contributor, dim_medical_center, dim_company, dim_desease, dim_date = query_dimensions()

    merged_df = merged_df_aux.join(dim_contributor.select("cotizante_id", "cedula"), merged_df_aux["id_usuario"]==dim_contributor["cedula"], "inner")
    merged_df = merged_df.join(dim_medical_center.select("centro_medico_id", "id_ips"), "id_ips", "inner")
    merged_df = merged_df.join(dim_company.select("empresa_id", "nit"), merged_df["empresa"]==dim_company["nit"], "left")
    merged_df = merged_df.join(dim_desease, merged_df["enfermedad"]==dim_desease["nombre"], "inner")
    merged_df = merged_df.join(dim_date.select("fecha_id", "fecha"), merged_df["fecha_pago"]==dim_date["fecha"], "inner")

    fact_facturacion = merged_df.select("fecha_id", "centro_medico_id", "enfermedad_id", "empresa_id", "cotizante_id", "valor_pagado")
    fact_facturacion = fact_facturacion.fillna(0, subset=["empresa_id"])
    insert_data(fact_facturacion.toPandas(), "fact_facturacion", URL, KEY) 
