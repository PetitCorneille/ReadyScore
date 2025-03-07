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
    #pdb.set_trace()
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
    def calculate_bonus_malus(df):
    # Appliquer les conditions de crédit pour déterminer min_loan et max_loan
        df = df.withColumn(
            "min_loan", 
            when(col("Nano_Loan").isNotNull(), 20)
            .when(col("Macro_Loan").isNotNull(), 25)
            .when(col("Advanced_Credit").isNotNull(), 100)
            .when(col("Cash_Roller_Over").isNotNull(), 100)
            .otherwise(0)
        )
        
        df = df.withColumn(
            "max_loan", 
            when(col("Nano_Loan").isNotNull(), 45)
            .when(col("Macro_Loan").isNotNull(), 250)
            .when(col("Advanced_Credit").isNotNull(), 500)
            .when(col("Cash_Roller_Over").isNotNull(), 500)
            .otherwise(0)
        )

        # Calculer le label "Repayment_Label" basé sur les balances
        df = df.withColumn(
            "Repayment_Label", 
            when((col("Balance_First") <= 0) | (col("Balance_Second") <= 0), "Strong ability to borrow")
            .when((col("Balance_First") > 0) & (col("Balance_Second") > 0), "Strong repayment capacity")
            .when((col("Balance_First") > 0) & (col("Balance_Second") <= 0), "Ability to borrow")
            .otherwise("Uncertain")
        )
        
        # Calcul du bonus/malus
        df = df.withColumn(
            "Bonus_Malus", 
            when((col("Balance_First") <= 0) | (col("Balance_Second") <= 0), 
                col("max_loan") + (col("max_loan") * 0.2))  # 20% bonus
            .when((col("Balance_First") > 0) & (col("Balance_Second") > 0), 
                col("max_loan") - (col("max_loan") * 0.2))  # 20% malus
            .when((col("Balance_First") > 0) & (col("Balance_Second") <= 0), 
                col("max_loan") + (col("max_loan") * 0.1))  # 10% bonus
            .otherwise(0)  # Si aucune condition n'est remplie
        )
        
        df.drop('max_loan', 'min_loan')
    
        return df
    
    # Appliquer la fonction pour calculer les bonus/malus
    final_clients = calculate_bonus_malus(final_clients)
    # Sauvegarder les résultats
    return final_clients 



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
