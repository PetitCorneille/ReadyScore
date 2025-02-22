from pyspark.sql.functions import avg, to_date, when, col, lit, dayofmonth, last_day, row_number,udf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import math

import pandas as pd
import pdb

def process_transactions(input_file):
    """
    Traite les transactions pour calculer les soldes moyens au 15 et au dernier jour du mois précédent.
    
    Args:
        input_file (str): Chemin du fichier CSV contenant les données brutes des transactions.
        output_file (str): Chemin de sauvegarde du fichier résultant.
        
    Returns:
        None: Le fichier est sauvegardé dans le chemin spécifié.
    """
    # Charger les données
    transactions = input_file

    # Conversion de la colonne `transaction_date` en format datetime
    transactions = transactions.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))


    # Ajouter des colonnes nécessaires
    transactions = transactions.withColumn("day_of_month", dayofmonth(col("transaction_date")))
    transactions = transactions.withColumn("days_in_month", dayofmonth(last_day(col("transaction_date"))))

    # Créer des DataFrames pour nameOrig et nameDest
    sim_numbers_orig = transactions.select(col('nameOrig').alias('SIM_NUMBER'),'transaction_date',
                                           col('newbalanceOrig').alias('balance'))
    sim_numbers_dest = transactions.select(col('nameDest').alias('SIM_NUMBER'),'transaction_date',
                                           col('newbalanceOrig').alias('balance'))
    
    # Fusionner les deux DataFrames pour obtenir une vue complète
    transactions_with_sims = sim_numbers_orig.union(sim_numbers_dest)

    # Filtrer uniquement les transactions pour les dates du 15 et du dernier jour du mois
    transactions_with_sims = transactions_with_sims.withColumn("day_of_month", dayofmonth(col("transaction_date")))
    transactions_with_sims = transactions_with_sims.withColumn("days_in_month", dayofmonth(last_day(col("transaction_date"))))

    relevant_transactions = transactions_with_sims.filter((col("day_of_month") == 15) | (col("day_of_month") == col("days_in_month")))
    # Ajouter la colonne DATE_OF_THE_DAY pour l'agrégation
    relevant_transactions = relevant_transactions.withColumn("DATE_OF_THE_DAY", col("transaction_date"))

    # Grouper par SIM_NUMBER et DATE_OF_THE_DAY, puis calculer la moyenne des soldes
    #result = relevant_transactions.groupby(['SIM_NUMBER', 'DATE_OF_THE_DAY'])['balance'].mean().reset_index()
    result = relevant_transactions.groupBy('SIM_NUMBER' , 'DATE_OF_THE_DAY').agg(avg('balance').alias('balance'))

    # Sauvegarder les résultats
    return result #.compute().to_csv(output_file, index=False)


#process_transactions("/raw/real_transactions_with_dates.csv")


def update_transactions(transactions_file, clients_file):
    """
    Met à jour le fichier de transactions avec les SIM_NUMBER présents dans le fichier clients,
    standardise les dates et calcule les moyennes des soldes.

    Args:
        transactions_file (str): Chemin du fichier CSV des transactions.
        clients_file (str): Chemin du fichier CSV des clients finaux.
        output_file (str): Chemin de sauvegarde du fichier résultant.
        
    Returns:
        None: Le fichier est sauvegardé dans le chemin spécifié.
    """
    # Charger les données existantes
    transactions_previous_month = transactions_file
    final_clients = clients_file

    # Filtrer les transactions en fonction des SIM_NUMBER présents dans final_clients
    transactions_previous_month = transactions_previous_month.join(final_clients, on="SIM_NUMBER", how="inner")\
    .select(final_clients["*"],transactions_previous_month["balance"])
    #transactions_previous_month = transactions_previous_month.withColumnRenamed('DATE_OF_THE_DAY_TMP', 'DATE_OF_THE_DAY')
    transactions_previous_month = transactions_previous_month.withColumn("DATE_OF_THE_DAY", to_date('DATE_OF_THE_DAY', "yyyy-MM-dd"))

    # Ajouter une colonne fictive pour les dates standardisées (2024-11-15 et 2024-11-30)
    transactions_previous_month = transactions_previous_month.withColumn(
    "Standardized_Date",
        when(dayofmonth(col("DATE_OF_THE_DAY")) == 15, lit("2024-11-15").cast("date"))
        .when(dayofmonth(col("DATE_OF_THE_DAY")) == 30, lit("2024-11-30").cast("date"))
        .otherwise(None)
    )

    # Supprimer les valeurs manquantes dans la colonne 'balance'
    transactions_previous_month = transactions_previous_month.dropna(subset=['balance'])

    # Grouper par SIM_NUMBER et Standardized_Date, puis calculer la moyenne
    transactions_previous_month = transactions_previous_month.groupBy("SIM_NUMBER", "Standardized_Date")\
                                                             .agg(avg("balance").alias("balance"))

    # Renommer la colonne des dates standardisées pour conserver la structure demandée
    transactions_previous_month = transactions_previous_month.withColumnRenamed("Standardized_Date", "DATE_OF_THE_DAY")

    # Convertir la colonne DATE_OF_THE_DAY en datetime pour uniformité
    #transactions_previous_month['DATE_OF_THE_DAY'] = pd.to_datetime(transactions_previous_month['DATE_OF_THE_DAY'])
    ''' la conversion en date a été fait lors de la creation de la colonne'''

    # Réinitialiser l'index pour finaliser
    '''Je supprime la ligne qui gere l'index'''
    #window_spec = Window.orderBy("SIM_NUMBER") 
    #transactions_previous_month = transactions_previous_month.withColumn("index", row_number().over(window_spec) - 1)

    # Sauvegarder le fichier mis à jour
    return transactions_previous_month



#update_transactions(
#    transactions_file="processed/transactions_previous_month.csv",
#    clients_file="processed/final_clients.csv"
#)



def calculate_bonus_malus(input_file, final_clients_file):
    """
    Calcule les bonus/malus des clients à partir des transactions et sauvegarde les résultats dans un fichier.
    
    Args:
        input_file (str): Chemin du fichier CSV contenant les transactions précédentes.
        final_clients_file (str): Chemin du fichier CSV contenant les informations des clients finaux.
        output_file (str): Chemin de sauvegarde du fichier résultant.
        
    Returns:
        None: Le fichier est sauvegardé dans le chemin spécifié.
    """
    # Charger les données
    transactions_previous_month = input_file
    final_clients = final_clients_file

    # Convertir la colonne DATE_OF_THE_DAY en datetime
    transactions_previous_month = transactions_previous_month.withColumn("DATE_OF_THE_DAY", to_date(col("DATE_OF_THE_DAY"), "yyyy-MM-dd"))
    # Filtrer les lignes pour le 15 et le dernier jour
    balance_15 = transactions_previous_month.filter(dayofmonth(col("DATE_OF_THE_DAY")) == 15) \
             .select(col("SIM_NUMBER"), col("balance").alias("Balance_First"))
    balance_30 = transactions_previous_month.filter(dayofmonth(col("DATE_OF_THE_DAY")) == 30) \
             .select(col("SIM_NUMBER"), col("balance").alias("Balance_Second"))
    # Fusionner les balances avec final_clients

    final_clients = final_clients.join(balance_15, on="SIM_NUMBER", how="left")
    final_clients = final_clients.join(balance_30, on="SIM_NUMBER", how="left")

    # Calcul des bonus/malus
    result_schema = StructType([
        StructField("Repayment_Label", StringType(), True),
        StructField("Bonus_Malus", DoubleType(), True)
    ])

    # Définition de l'UDF en PySpark
    @udf(returnType=result_schema)
    def calculate_bonus_malus(nano_loan, macro_loan, advanced_credit, cash_roller_over, balance_first, balance_second):
        min_loan, max_loan = 0, 0

        # Fonction utilitaire pour vérifier que la valeur n'est ni None ni NaN
        def is_valid(val):
            return val is not None and not (isinstance(val, float) and math.isnan(val))

        # Détermination des bornes de crédit selon le type de crédit
        if is_valid(nano_loan):
            min_loan, max_loan = 20, 45
        elif is_valid(macro_loan):
            min_loan, max_loan = 25, 250
        elif is_valid(advanced_credit):
            min_loan, max_loan = 100, 500
        elif is_valid(cash_roller_over):
            min_loan, max_loan = 100, 500

        # Application des règles de bonus/malus
        if balance_first <= 0 or balance_second <= 0:
            repayment_label = "Strong ability to borrow"
            bonus_malus = max_loan + (max_loan * 20 / 100)
        elif balance_first > 0 and balance_second > 0:
            repayment_label = "Strong repayment capacity"
            bonus_malus = max_loan - (max_loan * 20 / 100)
        elif balance_first > 0 and balance_second <= 0:
            repayment_label = "Ability to borrow"
            bonus_malus = max_loan + (max_loan * 10 / 100)  # ajustement de 10%
        else:
            repayment_label = "Uncertain"
            bonus_malus = 0.0

        return (repayment_label, bonus_malus)

    # Application de l'UDF sur le DataFrame final_clients
    # On crée une nouvelle colonne temporaire "bonus_result" de type struct, puis on extrait ses champs
    df_with_bonus = final_clients.withColumn("bonus_result", calculate_bonus_malus(
        col("Nano_Loan"),
        col("Macro_Loan"),
        col("Advanced_Credit"),
        col("Cash_Roller_Over"),
        col("Balance_First"),
        col("Balance_Second")
    ))

    # Extraction des colonnes et suppression de la colonne temporaire
    df_with_bonus = df_with_bonus.withColumn("Repayment_Label", col("bonus_result.Repayment_Label")) \
                                .withColumn("Bonus_Malus", col("bonus_result.Bonus_Malus")) \
                                .drop("bonus_result")
    # Sauvegarder les résultats
    return df_with_bonus #final_clients.to_csv(output_file, index=False)



#calculate_bonus_malus(
#    input_file="processed/transactions_previous_month.csv",
#    final_clients_file="processed/final_clients.csv"
#)



def update_loans_with_bonus_malus(input_file):
    """
    Met à jour les colonnes des prêts avec les bonus/malus calculés et sauvegarde le fichier résultant.

    Args:
        input_file (str): Chemin du fichier CSV contenant les clients avec bonus/malus.
        output_file (str): Chemin de sauvegarde du fichier résultant.

    Returns:
        None: Le fichier est sauvegardé dans le chemin spécifié.
    """
    # Charger les données
    final_clients = input_file

    # Mettre à jour les colonnes des prêts avec les bonus/malus
    
    for loan_type in ['Nano_Loan', 'Advanced_Credit', 'Macro_Loan', 'Cash_Roller_Over']:
        updated_column = f"{loan_type}_updated"
        final_clients = final_clients.withColumn(updated_column, col(loan_type) + col('Bonus_Malus'))

    # Sauvegarder le fichier avec les colonnes mises à jour
    return final_clients #.to_csv(output_file, index=False)



#update_loans_with_bonus_malus(
#    input_file="processed/final_clients_with_bonus_malus.csv"
#)


# Exécutable pour tester indépendamment
if __name__ == "__main__":
    print("ça ne s'appelle pas de cette façon.")
