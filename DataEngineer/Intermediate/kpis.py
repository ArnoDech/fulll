from pipeline import SparkIntegration
from pyspark.sql.functions import col, sum, coalesce
from pyspark.sql.window import Window


class kpis(SparkIntegration):

    def sales_on_14012022(self):
        """
        Affiche le nombre de transactions ayant eu lieu le 14/01/2022.
        """
        if self.db is None:
            print("La base de données n'a pas été chargée.")
            return

        try:
            count_sales_14012022 = self.db.filter(self.db.transaction_date== "2022-01-14").count()
            print(f"le nombre de transaction ayant eu lieu le 14/01/2022 est de : {count_sales_14012022}")

        except Exception as e:
            print(f"Erreur lors du calcul des ventes : {e}")


    def sum_sales(self):
        """
         Affiche la somme totale des ventes.
         """
        if self.db is None:
            print("La base de données n'a pas été chargée.")
            return

        try:
            db_filtered = self.db.filter(self.db.category == "SELL")
            sum_sales = db_filtered.agg(sum("amount_inc_tax").alias("total_value")).collect()[0]["total_value"]
            print(f"le total des ventes est de : {sum_sales}")

        except Exception as e:
            print(f"Erreur lors du calcul de la somme des ventes : {e}")


    def balance_amazon_sell_buy(self):
        """
        Affiche la balance entre les ventes et les achats pour le produit 'Amazon Echo Dot'.
        """
        if self.db is None:
            print("La base de données n'a pas été chargée.")
            return

        try:

            #Dataframe des ventes Amazon Echo Dot
            sales_filtered_df = self.db.filter((col("name") == "Amazon Echo Dot") & (col("category") == "SELL"))
            sales = sales_filtered_df.groupBy("transaction_date").agg(
                sum(col("amount_inc_tax")).alias("total_sales")
            )
            sales = sales.na.fill({"total_sales": 0})


            #Dataframe des achats Amazon Echo Dot
            buy_filtered_df = self.db.filter((col("name") == "Amazon Echo Dot") & (col("category") == "BUY"))
            buy = buy_filtered_df.groupBy("transaction_date").agg(
                sum(col("amount_inc_tax")).alias("total_bought")
            )

            result = sales.join(buy, "transaction_date", "outer")

            #Dataframe balance ventes-achats
            balance = result.withColumn("balance", coalesce(col("total_sales"), col("total_bought") * 0) - coalesce(col("total_bought"), col("total_sales") * 0))
            balance = balance.orderBy(col("transaction_date").asc())

            print("*****************   Dataframe Balance SELL-BUY/transaction date   **********************")
            balance.show()

            #OPTIONAL: Calcul de la balance cumulative !
            window_spec= Window.orderBy(col("transaction_date").asc())

            # Ajout de la colonne de la somme cumulée dans le Dataframe et affichage du résultat
            cumulative_balance = balance.withColumn("cumulative_balance", sum("balance").over(window_spec))
            print("*****************   Dataframe Cumulative Balance SELL-BUY/transaction date   **********************")
            cumulative_balance.show()

        except Exception as e:
            print(f"Erreur lors du calcul de la balance : {e}")

