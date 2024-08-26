from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class SparkIntegration:


    def __init__(self):
        self.spark = None
        self.table_to_ingest = None
        self.db=None
        self.balance=None


    def spark_session(self)-> None:
        """
        Création de la session Spark et configuration des options nécessaires.
        """
        self.spark = SparkSession.builder \
            .appName("Integration") \
            .config("spark.jars", "sqlite-jdbc-3.34.0.jar") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")


    def read_dataframe(self,path: str)-> None:
        """
        Lit un fichier CSV et le charge dans la variable dataframe self.table_to_ingest.

        :param path: Chemin vers le fichier CSV.
        """
        self.table_to_ingest= self.spark.read.csv(path, header=True, inferSchema=True)


    def add_fixed_date_column(self, column_name:str, date_value:str)-> None:
        """
        Ajoute une colonne de date fixe au DataFrame.

        :param column_name: Nom de la colonne à ajouter.
        :param date_value: Valeur de la date à ajouter.
        """
        self.table_to_ingest = self.table_to_ingest.withColumn(column_name, lit(date_value))


    def column_at_position(self, column_name:str, position:str)-> None:
        """
          Réorganise les colonnes du DataFrame en mettant une colonne spécifique à une position donnée.

          :param column_name: Nom de la colonne à repositionner.
          :param position: Position cible pour la colonne.
          """
        cols = self.table_to_ingest.columns
        reordered_cols = cols[:position - 1] + [column_name] + cols[position - 1:-1]
        self.table_to_ingest  = self.table_to_ingest.select(*reordered_cols)


    def rename_column(self, old_column_name:str, new_column_name:str)-> None:
        """
        Renomme une colonne dans le DataFrame.

        :param old_column_name: Ancien nom de la colonne.
        :param new_column_name: Nouveau nom de la colonne.
        """
        self.table_to_ingest= self.table_to_ingest.withColumnRenamed(old_column_name, new_column_name)
        print("Data récupérées du fichier CSV, nettoyées et transformées")
        self.table_to_ingest.show()


    def read_db(self,db_name:str)-> None:
        """
        Lit une base de données SQLite et la charge dans self.db.

        :param db_name: Nom de la base de données SQLite.
        """
        try:
            self.db = self.spark.read.format("jdbc") \
                .option("url", f"jdbc:sqlite:{db_name}") \
                .option("dbtable", "transactions") \
                .option("driver", "org.sqlite.JDBC") \
                .load()

            num_rows = self.db.count()

            print(f"Nombre de lignes dans la base de données est de : {num_rows}")

        except Exception as e:
            print(f"Erreur lors de la lecture de la base de données : {e}")


    def add_data(self)-> None:
        """
        Ajoute les données du DataFrame à une base de données SQLite.
        """
        if self.table_to_ingest is None:
            print("Aucun DataFrame à ajouter !")
            return

        try:
            self.table_to_ingest.write.format("jdbc") \
                .option("url", "jdbc:sqlite:retail.db") \
                .option("dbtable", "transactions") \
                .option("driver", "org.sqlite.JDBC") \
                .mode("append") \
                .save()

            updated_row_number = self.db.count()

            print(f"Le nouveau nombre de lignes dans le DataFrame : {updated_row_number}")

        except Exception as e:
            print(f"Erreur lors de l'ajout des données : {e}")






