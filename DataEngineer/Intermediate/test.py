from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class SparkIntegration:


    def __init__(self):
        self.spark = None
        self.table_to_ingest = None


    def spark_session(self, master_url):
        # Créer la session Spark en se connectant à un cluster Spark existant
        self.spark = SparkSession.builder \
            .appName("Integration") \
            .getOrCreate()


    def read_dataframe(self,path):
        self.table_to_ingest= self.spark.read.csv(path, header=True, inferSchema=True)
        self.table_to_ingest.show()


    def add_fixed_date_column(self, column_name, date_value):
        self.table_to_ingest = self.table_to_ingest.withColumn(column_name, lit(date_value))
        self.table_to_ingest.show()



if __name__ == '__main__':
    test = SparkIntegration()
    test.spark_session(master_url="http://localhost:8080")
    test.read_dataframe("retail_15_01_2022.csv")
    test.add_fixed_date_column("transaction_date","15-01-2022")
