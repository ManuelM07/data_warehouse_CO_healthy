from conection import execute_query
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (monotonically_increasing_id, lit, split, 
                                   explode, col, udf)
from sodapy import Socrata

# Define all the dimensions involved in the processes

class Dimension:

    def __init__(self) -> None:
        self.spark = SparkSession.builder.appName('Transformx').getOrCreate()


    def beneficiary(self):
        result, column_names = execute_query("SELECT * FROM beneficiario")
        df_beneficiary = self.spark.createDataFrame(result, column_names)
        df_beneficiary = df_beneficiary.select('id_beneficiario', 'nombre', 'fecha_nacimiento', 'sexo')
        return df_beneficiary.dropDuplicates()
    

    def dim_contributor(self):
        result, column_names = execute_query("SELECT * FROM cotizante")
        df_contributor = self.spark.createDataFrame(result, column_names)
        df_contributor = df_contributor.drop("id_ips", "direccion", "estado_civil", "estracto", "tipo_discapacidad", "salario_base")

        return df_contributor.dropDuplicates()
    

    def dim_user(self):
        df_contributor_aux = self.dim_contributor()
        df_beneficiary = self.beneficiary()
        df_contributor_aux = df_contributor_aux.select('cedula', 'nombre', 'fecha_nacimiento', 'sexo')
        
        # Concat df beneficiary & contributor for create dim user
        df_contributor_aux = df_contributor_aux.withColumnRenamed("cedula", "identificacion")
        df_beneficiary = df_beneficiary.withColumnRenamed("id_beneficiario", "identificacion")
        df_merged_user = df_contributor_aux.unionByName(df_beneficiary)

        return df_merged_user.dropDuplicates()
    

    def dim_medical_center(self):
        result, column_names = execute_query("SELECT * FROM ips")
        df_medical_center = self.spark.createDataFrame(result, column_names)
        df_medical_center = df_medical_center[['id_ips', 'nombre', 'direccion', 'tipo_ips', 
                'municipio']].withColumnRenamed("tipo_ips", 
                "tipo_centro_medico").withColumn("activo", lit(True))
        return df_medical_center.dropDuplicates()
    
    
    def dim_medico(self):
        result, column_names = execute_query("SELECT * FROM medico")
        df_medico = self.spark.createDataFrame(result, column_names)
        df_medico = df_medico.drop("subespecialidad", "id_ips", "Direccion_Consultorio")

        return df_medico.dropDuplicates()
    

    def dim_medicine(self):
        medicine_path = "medicamentos.xls"
        df_medicine = pd.read_excel(medicine_path)
        df_medicine = self.spark.createDataFrame(df_medicine)
        df_medicine = df_medicine.drop("Forma Farmacéutica")

        # rename names in dim medicine
        column_rename_list = [("Código", "codigo"), ("Nombre Genérico", "nombre"), ("Presentación", "presentacion"),
                            ("Laboratorio y Registro", "laboratorio_registro"), ("Precio", "precio"), ("Tipo Medicamento", "tipo_medicamento")]
        for old_name, new_name in column_rename_list:
            df_medicine = df_medicine.withColumnRenamed(old_name, new_name)

        return df_medicine.dropDuplicates()


    def dim_date(self, start_date, end_date):
        df = pd.DataFrame({"fecha": pd.date_range(start_date, end_date)})

        # Map the weekday names in spanish
        weekday = {
            0: "Lunes",
            1: "Martes",
            2: "Miércoles",
            3: "Jueves",
            4: "Viernes",
            5: "Sábado",
            6: "Domingo"
        }

        # Map the month names in spanish
        months = {
            1: "enero",
            2: "febrero",
            3: "marzo",
            4: "abril",
            5: "mayo",
            6: "junio",
            7: "julio",
            8: "agosto",
            9: "septiembre",
            10: "octubre",
            11: "noviembre",
            12: "diciembre"
        }

        df["anio"] = df["fecha"].dt.year
        df["mes_numero"] = df["fecha"].dt.month
        df["mes"] = df["fecha"].dt.month.map(months)
        df["dia_numero"] = df["fecha"].dt.day
        df["dia"] = df["fecha"].dt.day
        df["dia_semana"] = df["fecha"].dt.weekday.map(weekday)
        df["es_fin_de_semana"] = df["fecha"].dt.weekday.isin([5, 6])

        # Get result
        df["fecha"] = df["fecha"].dt.strftime('%Y-%m-%d')  # Format of fecha "yyyy-MM-dd"
        df = df[["fecha", "anio", "mes_numero", "mes", "dia_numero", "dia_semana", "es_fin_de_semana"]]
        
        return self.spark.createDataFrame(df)

    
    def dim_region(self):
        # Conection to API of Socrata
        socrata_domain = 'www.datos.gov.co'
        socrata_dataset_id = 'xdk5-pm3f'

        # Create client Socrata
        client = Socrata(socrata_domain, None)

        results = client.get(socrata_dataset_id, limit=1200)
        df_region = self.spark.createDataFrame(results)
        df_region = df_region.select("municipio", "departamento", "region")

        return df_region.dropDuplicates()


    def dim_disease(self):
        result, column_names = execute_query("SELECT * FROM preexistencias")
        df_disease = self.spark.createDataFrame(result, column_names)
        df_disease = df_disease[["enfermedad"]].distinct()
        return df_disease.dropDuplicates()
    

    def dim_company(self):
        result, column_names = execute_query("SELECT * FROM empresa")
        df_company = self.spark.createDataFrame(result, column_names)
        return df_company.dropDuplicates()


    def dim_demographic(self):
        result, column_names = execute_query("SELECT * FROM cotizante")
        df_contributor = self.spark.createDataFrame(result, column_names)
        df_contributor = df_contributor.select("direccion", "estado_civil", "estracto", "tipo_discapacidad", "salario_base")

        return df_contributor.dropDuplicates()
