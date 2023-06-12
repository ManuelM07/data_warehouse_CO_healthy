from conection import execute_query, new_model
from dimension import Dimension
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from dotenv import dotenv_values

config = dotenv_values(".env")

spark = SparkSession.builder \
    .appName("Process") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

PROCESS = "retiro_process"


def get_retreat():
    """Get pricipal table"""
    
    result, column_names = execute_query("SELECT * FROM retiros")
    df_retreat = spark.createDataFrame(result, column_names)

    return df_retreat


def get_dimensions():
    """Get all dimensions to use"""

    ob_dimension = Dimension()
    df_medical_center = ob_dimension.dim_medical_center()
    df_region = ob_dimension.dim_region()
    df_contributor= ob_dimension.dim_contributor()
    df_demographic = ob_dimension.dim_demographic()
    df_date = ob_dimension.dim_date("2006-01-01", "2010-12-31")

    return df_medical_center, df_region, df_contributor, df_demographic, df_date


def query_dimensions_demografic():
    # Configuring the connection properties to CockroachDB
    aux_url = config['URL_JDBC']
    url = f"{aux_url}/{PROCESS}"

    properties = {
        "user": config['USER_DW'],
        "password": config['PASSWORD_DW'],
        "driver": "org.postgresql.Driver"
    }

    dim_demographic = spark.read.jdbc(url, "dim_demografica", properties=properties)

    return dim_demographic


def insert_data_dim(df_contributor):
    """Insert the tables in the db"""

    df_medical_center, df_region, _, _, df_date = get_dimensions()

    new_model(df_medical_center.toPandas(), "dim_centro_medico", PROCESS)
    new_model(df_region.toPandas(), "dim_region", PROCESS)
    new_model(df_contributor.toPandas(), "dim_cotizante", PROCESS)
    new_model(df_date.toPandas(), "dim_fecha", PROCESS)


def query_dimensions():
    # Configuring the connection properties to CockroachDB
    aux_url = config['URL_JDBC']
    url = f"{aux_url}/{PROCESS}"

    properties = {
        "user": config['USER_DW'],
        "password": config['PASSWORD_DW'],
        "driver": "org.postgresql.Driver"
    }

    dim_contributor = spark.read.jdbc(url, "dim_cotizante", properties=properties)
    dim_medical_center = spark.read.jdbc(url, "dim_centro_medico", properties=properties)
    dim_region = spark.read.jdbc(url, "dim_region", properties=properties)
    dim_date = spark.read.jdbc(url, "dim_fecha", properties=properties)

    return dim_contributor, dim_medical_center, dim_region, dim_date


def update_name_city(dim_medical_center):
    """Update the names of the cities that have tildes errors"""

    dim_medical_center = dim_medical_center.withColumn("municipio",
                              when(col("municipio") ==  "Bogota", "Bogotá D.C.")
                            .when(col("municipio") ==  "Bagad�", "Bagadó")
                            .when(col("municipio") ==  "Facatativ�", "Facatativá")
                            .when(col("municipio") ==  "Jamund�", "Jamundí")
                            .when(col("municipio") ==  "Medellin", "Medellín")
                            .when(col("municipio") ==  "Monter�a", "Montería")
                            .when(col("municipio") ==  "Nuqu�", "Nuquí")
                            .when(col("municipio") ==  "Quibd�", "Quibdó")
                            .when(col("municipio") ==  "Valpara�so", "Valparaíso")
                            .otherwise(col("municipio")))
    
    return dim_medical_center


def run():
    df_retreat = get_retreat()

    result, column_names = execute_query("SELECT * FROM COTIZANTE")
    df_contributor_aux = spark.createDataFrame(result, column_names)

    df_medical_center, df_region, df_contributor, df_demographic, df_date = get_dimensions()

    new_model(df_demographic.toPandas(), "dim_demografica", PROCESS)
    dim_demographic = query_dimensions_demografic()

    df_contributor = dim_demographic.join(df_contributor_aux, on=["direccion", "estado_civil", "estracto", "tipo_discapacidad", "salario_base"])
    df_contributor = df_contributor.select("cedula", "nombre", "tipo_cotizante", "sexo", "fecha_nacimiento", 
                                       "nivel_escolaridad", "fecha_afiliacion", "demografica_id", "proviene_otra_eps")

    # Conver number to boolean
    df_contributor = df_contributor.withColumn("proviene_otra_eps", when(col("proviene_otra_eps") == 1, True).otherwise(False))
    insert_data_dim(df_contributor)

    dim_contributor, dim_medical_center, dim_region, dim_date = query_dimensions()
    dim_medical_center = update_name_city(dim_medical_center)

    merged_df = df_retreat.join(df_contributor_aux.select("cedula", "id_ips"), df_retreat["id_usuario"]==df_contributor_aux["cedula"], "inner")
    merged_df = merged_df.join(dim_medical_center.select("centro_medico_id", "id_ips", "municipio"), "id_ips", "inner")
    merged_df = merged_df.join(dim_region, "municipio", "inner")
    merged_df = merged_df.join(dim_contributor.select("cotizante_id", "cedula"), "cedula", "inner")
    merged_df = merged_df.join(dim_date.select("fecha_id", "fecha"), merged_df["fecha_retiro"]==dim_date["fecha"], "inner")

    fact_retreat = merged_df.select("fecha_id", "region_id", "centro_medico_id", "cotizante_id", "cambio_a_eps")
    new_model(fact_retreat.toPandas(), "fact_retiro", PROCESS)
