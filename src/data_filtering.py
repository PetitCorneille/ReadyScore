
from pyspark.sql.functions import col,  row_number

import load_params

def filter_data(merged_data):
    """
    Pré-filtre la base de données fusionnée pour obtenir une base propre et fiable.

    Args:
        merged_data (pd.DataFrame): DataFrame fusionnée contenant les données utilisateur et KYC.

    Returns:
        pd.DataFrame: DataFrame filtrée selon les critères définis.
    """
    # Filtre 1 : Utilisateurs actifs dans les 90 derniers jours
    filtered_data = merged_data.filter(merged_data['HAS_USED_MOB_MONEY_IN_LAST_90_DAYS'] == 1)

    # Filtre 2 : Limiter les tranches d'âge à faible risque
    filtered_data = filtered_data.filter((col('age') >= load_params.tranche_age["min"]) & (col('age') <= load_params.tranche_age["max"]))

    # Filtre 3 : Inclure uniquement les clients entièrement conformes au KYC
    filtered_data = filtered_data.where(filtered_data['REGISTRATION_STATUS'] == 'Accepted')

    # Réinitialiser les index
    #filtered_data = filtered_data.reset_index(drop=True)
    
    filtered_data.write \
        .format("mongodb") \
        .option("database", load_params.spark_config["database"]) \
        .option("collection", "filtered_data") \
        .mode("overwrite") \
        .save()
    print("Fichier filtré sauvegardé dans la collection filtered_data")
    return  filtered_data

if __name__ == "__main__":  
    print('cete fonction ne s\'appele pas ainsi')