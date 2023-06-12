from conection import execute_query, new_model
from dimension import Dimension
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from dotenv import dotenv_values
import findspark
findspark.init()
config = dotenv_values(".env")

spark = SparkSession.builder \
    .appName("Process") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

PROCESS = "medical_formula_process"

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

    new_model(df_user.toPandas(), "dim_usuario", PROCESS)
    new_model(df_medico.toPandas(), "dim_medico", PROCESS)
    new_model(df_medicine.toPandas(), "dim_medicamento", PROCESS)
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

    df_user = spark.read.jdbc(url, "dim_usuario", properties=properties)
    df_medico = spark.read.jdbc(url, "dim_medico", properties=properties)
    df_medicine = spark.read.jdbc(url, "dim_medicamento", properties=properties)
    df_date = spark.read.jdbc(url, "dim_fecha", properties=properties)

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

    fact_medical_formula = merged_df.select("fecha_id", "usuario_id", "medico_id", "medicamento_id")

    new_model(fact_medical_formula.toPandas(), "fact_medical_formula", PROCESS)