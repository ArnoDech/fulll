from pipeline import SparkIntegration
from pyspark.sql.functions import *


class tests(SparkIntegration):


    def check_and_remove_duplicates(self):

        if not isinstance(self.db, DataFrame):
            raise TypeError("self.db doit être un DataFrame PySpark.")

        try :
            # Exemple : Supposons que df est votre DataFrame et column_name est la colonne à vérifier pour les doublons
            duplicated_df = self.db.groupBy("id").agg(count("id").alias("count")).filter("count > 1")

            nombre_de_doublons = duplicated_df.count()

            if nombre_de_doublons >0 : #on vient supprimer les doublons de la base de données !
                print(f"Il y a des doublons de id dans la base de données, cela concerne : {nombre_de_doublons} lignes.")
                self.db = self.db.dropDuplicates(["id"]) #suppression des doublons basés sur la colonne id
            else :
                print("pas de doublons")

        except Exception as e:
            print(f"Une erreur est survenue : {e}")

    def test_number_of_transactions_on_15_01_2022(self):
        if not isinstance(self.db, DataFrame):
            raise TypeError("self.db doit être un DataFrame PySpark.")

        try :
            transaction_on_15012022=self.db.filter(self.db.transaction_date=="2022-01-15")
            transaction_on_15012022 = transaction_on_15012022.count()
            expected_nb_transactions = 54
            if transaction_on_15012022 == expected_nb_transactions:
                print(f"Le nombre de transactions réalisées le 15-01-2022 est de : {expected_nb_transactions}, test validé")
            else :
                print(f"test non validé !")

        except Exception as e:
            print(f"Une erreur est survenue : {e}")

