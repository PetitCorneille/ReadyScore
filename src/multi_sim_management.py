from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf ,PandasUDFType
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import pdb

def identify_multi_sim_clients(data):
    """
    Identifie les clients ayant plusieurs cartes SIM avec la même pièce d'identité.

    Args:
        data (pd.DataFrame): DataFrame contenant les colonnes 'ID_TYPE', 'ID_NUMBER', et 'SIM_NUMBER'.

    Returns:
        pd.DataFrame: Clients ayant plus d'une carte SIM avec leurs informations.
    """
    multi_sim_clients = (
        data.groupBy('ID_TYPE', 'ID_NUMBER').agg(F.countDistinct('ID_TYPE', 'ID_NUMBER').alias('SIM_COUNT'))  
    )
    return multi_sim_clients.filter(multi_sim_clients['SIM_COUNT'] > 1)

def select_best_profile(group):
    """
    Sélectionne le meilleur profil pour un client ayant plusieurs SIM.

    Args:
        group (pd.DataFrame): Sous-ensemble de données pour un client.

    Returns:
        pd.Series: Ligne représentant le meilleur profil pour le client.
    """
    # Create a new column 'Segment_Score' based on the 'Segment' column
    group = group.withColumn('Segment_Score', F.when(group['Segment'] == 'Very Low', 1)
                             .when(group['Segment'] == 'Low', 2)
                             .when(group['Segment'] == 'Medium', 3)
                             .when(group['Segment'] == 'High', 4)
                             .when(group['Segment'] == 'Very High', 5)
                             .otherwise(None))
    # Create a new column 'Max_Credit' as the maximum of the specified columns
    group = group.withColumn('Max_Credit', F.greatest('Nano_Loan', 'Advanced_Credit', 'Macro_Loan', 'Cash_Roller_Over'))
    # Define a window specification to partition by 'ID_TYPE' and 'ID_NUMBER' and order by 'Segment_Score' and 'Max_Credit'
    window_spec = Window.partitionBy('ID_TYPE', 'ID_NUMBER').orderBy(F.desc('Segment_Score'), F.desc('Max_Credit'))
    # Add a row number to each row within its partition
    group = group.withColumn('row_num', F.dense_rank().over(window_spec))
    # Filter to get only the top-ranked row for each group
    best_profiles = group.filter(group['row_num'] == 1).drop('row_num')
    return best_profiles


def manage_multi_sim_clients(data):
    """
    Gère les clients avec plusieurs SIM en conservant uniquement le meilleur profil.

    Args:
        data (pd.DataFrame): DataFrame contenant les colonnes nécessaires pour la gestion des clients.

    Returns:
        pd.DataFrame: DataFrame finale avec une seule ligne par client.
    """
    # Identifier les clients avec plusieurs SIM
    multi_sim_clients = identify_multi_sim_clients(data)
    
    # Extraire les détails des clients multi-SIM
    multi_sim_details = data.join(
        multi_sim_clients.select('ID_TYPE', 'ID_NUMBER'), 
        on=['ID_TYPE', 'ID_NUMBER'], 
        how='inner'  
    )
    multi_sim_details = multi_sim_details.select(data.columns)

    # Appliquer la sélection du meilleur profil
    
    best_profiles = select_best_profile(multi_sim_details)

    # Identifier les clients à SIM unique
    single_sim_clients = data.join(multi_sim_clients.select('ID_TYPE', 'ID_NUMBER'), 
        on=['ID_TYPE', 'ID_NUMBER'], how='left' )
    single_sim_clients = single_sim_clients.withColumn("_merge", F.when(F.col('ID_TYPE').isNull(), 'both').otherwise('left_only'))
    
    single_sim_clients = single_sim_clients.filter(single_sim_clients['_merge'] == 'left_only')
    single_sim_clients = single_sim_clients.drop('_merge')
   
    best_profiles = best_profiles.drop('Segment_Score', 'Max_Credit')
    best_profiles = best_profiles.select(*single_sim_clients.columns)
    # Combiner les données pour créer la DataFrame finale
    final_clients = single_sim_clients.union(best_profiles)
    #final_clients = final_clients.withColumn('row_id', F.monotonically_increasing_id())
    final_clients.drop('Segment_Score', 'Max_Credit')
    return final_clients


def validate_final_clients(data):
    """
    Valide que chaque client est unique et associé à une seule SIM.

    Args:
        data (pd.DataFrame): DataFrame finale des clients.

    Returns:
        None
    """
    # Vérifier l'unicité des ID_NUMBER par ID_TYPE
    unique_clients = data.groupBy("ID_TYPE", "ID_NUMBER").count() 
    #data.groupby(['ID_TYPE', 'ID_NUMBER']).size()
    #non_unique_clients = unique_clients[unique_clients > 1]
    non_unique_clients = unique_clients.filter(unique_clients['count'] > 1)

    if not non_unique_clients.isEmpty():#.empty:
        print("Les clients suivants ont plusieurs lignes dans final_clients :")
        print(non_unique_clients.show())
    else:
        print("Toutes les lignes de final_clients représentent des clients uniques.")

    # Vérifier qu'il n'y a qu'une seule SIM par client
    
    multiple_sims = data.groupBy('ID_TYPE', 'ID_NUMBER') \
                    .agg(F.countDistinct('SIM_NUMBER').alias('unique_SIM_NUMBER'))
    non_unique_sims = multiple_sims.filter(multiple_sims['unique_SIM_NUMBER'] > 1)

    if not non_unique_sims.isEmpty():#.empty:
        print("Les clients suivants sont associés à plusieurs SIMs dans final_clients :")
        print(non_unique_sims.show())
    else:
        print("Chaque client dans final_clients est associé à une seule SIM.")
