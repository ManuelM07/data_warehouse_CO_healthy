from conection import execute_query, new_model
from dimension import Dimension
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when
from datetime import datetime
import pandas as pd
from dotenv import dotenv_values
config = dotenv_values(".env")


spark = SparkSession.builder \
    .appName('Transformx') \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

PROCESS = "servicio_process"


def get_services():
    """Get pricipal tables"""

    result, column_names = execute_query("SELECT * FROM CITAS_GENERALES")
    pandas_appointment = pd.DataFrame(result, columns=column_names)
    pandas_appointment[["hora_solicitud", "hora_atencion"]] = pandas_appointment[["hora_solicitud", "hora_atencion"]].astype(str)
    df_appointment = spark.createDataFrame(pandas_appointment)

    result, column_names = execute_query("SELECT * FROM URGENCIAS")
    pandas_urgency = pd.DataFrame(result, columns=column_names)
    pandas_urgency[["hora_solicitud", "hora_atencion"]] = pandas_urgency[["hora_solicitud", "hora_atencion"]].astype(str)
    df_urgency = spark.createDataFrame(pandas_urgency)

    result, column_names = execute_query("SELECT * FROM HOSPITALIZACIONES")
    pandas_hospitalizations = pd.DataFrame(result, columns=column_names)
    pandas_hospitalizations[["hora_solicitud", "hora_atencion"]] = pandas_hospitalizations[["hora_solicitud", "hora_atencion"]].astype(str)
    df_hospitalizations = spark.createDataFrame(pandas_hospitalizations)

    result, column_names = execute_query("SELECT * FROM REMISIONES")
    pandas_remission = pd.DataFrame(result, columns=column_names)
    pandas_remission[["hora_remision", "hora_atencion"]] = pandas_remission[["hora_remision", "hora_atencion"]].astype(str)
    df_remission = spark.createDataFrame(pandas_remission)

    return df_appointment, df_urgency, df_hospitalizations, df_remission


def get_dimensions():
    """Get all dimensions to use"""

    ob_dimension = Dimension()
    df_medico = ob_dimension.dim_medico()
    df_user = ob_dimension.dim_user()
    df_region = ob_dimension.dim_region()
    df_date = ob_dimension.dim_date("2006-01-01", "2010-12-31")

    return df_medico, df_user, df_region, df_date


def insert_data_dim():
    """Insert the tables in the db"""

    df_medico, df_user, df_region, df_date = get_dimensions()

    df_medico = df_medico.drop("direccion_consultorio")
    new_model(df_medico.toPandas(), "dim_medico", PROCESS)
    new_model(df_user.toPandas(), "dim_usuario", PROCESS)
    new_model(df_date.toPandas(), "dim_fecha", PROCESS)
    new_model(df_region.toPandas(), "dim_region", PROCESS)


def update_name_city():
    """Update the names of the cities that have tildes errors"""

    result, column_names = execute_query("SELECT * FROM IPS")
    df_medical_center = spark.createDataFrame(result, column_names)

    df_medical_center = df_medical_center.withColumn("municipio",
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
    
    return df_medical_center


def query_dimensions():
    # Configuring the connection properties to CockroachDB
    aux_url = config['URL_JDBC']
    url = f"{aux_url}/{PROCESS}"

    properties = {
        "user": config['USER_DW'],
        "password": config['PASSWORD_DW'],
        "driver": "org.postgresql.Driver"
    }

    dim_medico = spark.read.jdbc(url, "dim_medico", properties=properties)
    dim_user = spark.read.jdbc(url, "dim_usuario", properties=properties)
    dim_region = spark.read.jdbc(url, "dim_region", properties=properties)
    dim_hour = spark.read.jdbc(url, "dim_hora", properties=properties)
    dim_date = spark.read.jdbc(url, "dim_fecha", properties=properties)

    return dim_medico, dim_user, dim_region, dim_hour, dim_date


def run():
    # Init ETL

    df_appointment, df_urgency, df_hospitalizations, df_remission = get_services()

    insert_data_dim()

    result, column_names = execute_query("SELECT * FROM SERVICIOS_POS")
    df_pos_service = spark.createDataFrame(result, column_names)
    
    df_remission = df_remission.join(df_pos_service, df_remission["servicio_pos"]==df_pos_service["id_servicio_pos"], "inner")

    # Leave only the columns that we will use
    df_appointment = df_appointment.drop("codigo_cita")
    df_urgency = df_urgency.drop("codigo_urgencia")
    df_hospitalizations = df_hospitalizations.drop("codigo_hospitalizacion", "duracion_hospitalizacion")
    df_remission = df_remission.drop("codigo_remision", "id_medico_remite", "diagnostico", "servicio_pos", "id_servicio_pos", "costo")
    df_remission = df_remission.withColumnRenamed("descripcion", "diagnostico").withColumnRenamed("hora_remision", "hora_solicitud").withColumnRenamed("fecha_remision", "fecha_solicitud")

    # Create column tipo_servicio 
    df_appointment = df_appointment.withColumn("tipo_servicio", lit("Cita General"))
    df_urgency = df_urgency.withColumn("tipo_servicio", lit("Urgencia"))
    df_hospitalizations = df_hospitalizations.withColumn("tipo_servicio", lit("Hospitalización"))
    df_remission = df_remission.withColumn("tipo_servicio", lit("Remision"))

    merged_df_aux = df_appointment.union(df_urgency).union(df_hospitalizations).union(df_remission)

    result, column_names = execute_query("SELECT * FROM MEDICO")
    df_medico_aux = spark.createDataFrame(result, column_names)

    # Get region
    df_medical_center = update_name_city()
    region_aux = df_medico_aux.join(df_medical_center.select("id_ips", "municipio"), "id_ips", "inner")

    dim_medico, dim_user, dim_region, dim_hour, dim_date = query_dimensions()

    merged_df = merged_df_aux.join(dim_user.select("usuario_id", "identificacion"), merged_df_aux["id_usuario"]==dim_user["identificacion"], "inner")
    merged_df = merged_df.join(dim_medico.select("medico_id", "cedula"), merged_df["id_medico"]==dim_medico["cedula"], "inner")
    merged_df = merged_df.join(dim_hour.select("hora_id", "hora_label"), merged_df["hora_solicitud"]==dim_hour["hora_label"], "inner").withColumnRenamed("hora_id", "hora_solicitud_id").withColumnRenamed("hora_label", "hora_label_a")
    merged_df = merged_df.join(dim_hour.select("hora_id", "hora_label"), merged_df["hora_atencion"]==dim_hour["hora_label"], "inner").withColumnRenamed("hora_id", "hora_atencion_id")
    merged_df = merged_df.join(dim_date.select("fecha_id", "fecha"), merged_df["fecha_solicitud"]==dim_date["fecha"], "inner").withColumnRenamed("fecha_id", "fecha_solicitud_id").withColumnRenamed("fecha", "fecha_a")
    merged_df = merged_df.join(dim_date.select("fecha_id", "fecha"), merged_df["fecha_atencion"]==dim_date["fecha"], "inner").withColumnRenamed("fecha_id", "fecha_atencion_id")
    merged_df = merged_df.join(region_aux.select("cedula", "municipio"), "cedula", "left")
    merged_df = merged_df.join(dim_region, "municipio", "inner")

    fact_servicio = merged_df.select("fecha_solicitud_id", "hora_solicitud_id", "fecha_atencion_id", "hora_atencion_id", 
                                 "usuario_id", "medico_id", "region_id", "tipo_servicio", "diagnostico")

    new_model(fact_servicio.toPandas(), "fact_service", PROCESS)