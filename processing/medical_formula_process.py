from conection import execute_query, insert_data
from dimension import Dimension
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from dotenv import dotenv_values
import pandas as pd
#import findspark
#findspark.init()
config = dotenv_values(".env")

spark = SparkSession.builder \
    .appName("Process") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

PROCESS = "medical_formula_process"
URL = config['URL_MEDICAL_FORMULA']
KEY = config['KEY_MEDICAL_FORMULA']

def get_medical_formula():
    """Get pricipal table"""

    result, column_names = execute_query("SELECT * FROM formulas_medicas")
    df_medical_formula = spark.createDataFrame(result, column_names)

    df_medical_formula = df_medical_formula.withColumn("medicamentos_recetados", split(df_medical_formula["medicamentos_recetados"], ";"))
    df_medical_formula = df_medical_formula.withColumn("medicamento_id", explode(df_medical_formula["medicamentos_recetados"]))
    df_medical_formula = df_medical_formula.withColumnRenamed("medicamento_id", "medicamento_idx")

    return df_medical_formula


def get_dimensions():
    """Get all dimensions to use"""

    ob_dimension = Dimension()
    df_user = ob_dimension.dim_user()
    df_medico = ob_dimension.dim_medico()
    df_medicine = ob_dimension.dim_medicine()
    df_date = ob_dimension.dim_date("2006-01-01", "2010-12-31")

    return df_user, df_medico, df_medicine, df_date


def insert_data_dim():
    """Insert the tables in the db"""

    df_user, df_medico, df_medicine, df_date = get_dimensions()

    df_user = df_user.toPandas()

    # Change type date to str
    df_user['fecha_nacimiento'] = pd.to_datetime(df_user['fecha_nacimiento'])
    df_user['fecha_nacimiento'] = df_user['fecha_nacimiento'].dt.strftime("%Y-%m-%d")
    #df_date['fecha'] = df_date['fecha'].dt.strftime("%Y-%m-%d")

    #insert_data(df_user, "dim_usuario", URL, KEY)
    #insert_data(df_medico.toPandas(), "dim_medico", URL, KEY)
    #insert_data(df_medicine.toPandas(), "dim_medicamento", URL, KEY)
    #insert_data(df_date.toPandas(), "dim_fecha", URL, KEY)


def query_dimensions():
    # Configuring the connection properties to CockroachDB
    url = config['JDBC_MEDICAL_FORMULA']
    options = {
        "user": config['USER_OUT'],
        "password": config['PASSWORD_OUT'],
        "driver": "org.postgresql.Driver"
    }

    df_user = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "dim_usuario") \
        .options(**options) \
        .load()

    df_medico = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "dim_medico") \
        .options(**options) \
        .load()

    df_medicine = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "dim_medicamento") \
        .options(**options) \
        .load()

    df_date = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "dim_fecha") \
        .options(**options) \
        .load()

    """df_user = spark.read.jdbc(url, "dim_usuario", properties=properties)
    df_medico = spark.read.jdbc(url, "dim_medico", properties=properties)
    df_medicine = spark.read.jdbc(url, "dim_medicamento", properties=properties)
    df_date = spark.read.jdbc(url, "dim_fecha", properties=properties)"""

    return df_user, df_medico, df_medicine, df_date


def run():
    # Init ETL

    df_medical_formula = get_medical_formula()

    insert_data_dim()

    df_user, df_medico, df_medicine, df_date = query_dimensions()

    merged_df = df_medical_formula.join(df_user, df_user["identificacion"]==df_medical_formula["id_usuario"], "inner")
    merged_df = merged_df.join(df_medico, merged_df["id_medico"]==df_medico["cedula"], "inner")
    merged_df = merged_df.join(df_medicine, merged_df["medicamento_idx"]==df_medicine["codigo"], "inner")
    merged_df = merged_df.join(df_date, merged_df["fecha"]==df_date["fecha"], "inner")

    fact_medical_formula = merged_df.select("fecha_id", "usuario_id", "medico_id", "medicamento_id", "Codigo_Formula")
    fact_medical_formula.write.csv("file_extra/fact_medical_formula", header=True, mode="overwrite")


    #insert_data(fact_medical_formula.toPandas(), "fact_medical_formula", URL, KEY)