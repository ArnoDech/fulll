from pipeline import SparkIntegration
from kpis import kpis
from test import tests


def main() -> None:
    """
    Point d'entrée principal du script
    """
    try:

        #instanciation des classes SparkIntegration, KPIs et tests
        pipeline = SparkIntegration()
        kpi = kpis()
        test = tests()

        #Data integration
        pipeline.spark_session()
        pipeline.read_dataframe("retail_15_01_2022.csv")
        pipeline.add_fixed_date_column("transaction_date", "2022-01-15")
        pipeline.column_at_position("transaction_date", 2)
        pipeline.rename_column("description", "name")
        pipeline.read_db("retail.db")
        pipeline.add_data()

        #passage de variables
        test.db=pipeline.db

        # vérification si doublons dans la base de données et suppression des doublons
        test.check_and_remove_duplicates()
        test.test_number_of_transactions_on_15_01_2022()

        #passage de variables (base de données nettoyée)
        kpi.db = test.db

        # Calcul des KPIs
        kpi.sales_on_14012022()
        kpi.sum_sales()
        kpi.balance_amazon_sell_buy()

    except Exception as e:
        print(f"Une erreur est survenue : {e}")

if __name__ == "__main__":
    main()