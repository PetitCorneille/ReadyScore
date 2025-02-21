
from pyspark.sql.functions import to_date , col, current_date, datediff, floor

import load_params


def load_and_merge_data(user_data_path, kyc_data_path):
    """
    Load user data and KYC data, preprocess them, and merge into a single dataframe.

    Args:
        user_data_path (str): Path to the user data CSV file.
        kyc_data_path (str): Path to the KYC data CSV file.

    Returns:
        pd.DataFrame: Merged and preprocessed dataframe.
    """
    # Load the datasets
    user_data_df = user_data_path
    kyc_data_df  = kyc_data_path
    # Ensure date columns are properly formatted
    kyc_data_df = kyc_data_df.withColumn("BIRTH_DATE", to_date(kyc_data_df["BIRTH_DATE"], "yyyy-MM-dd"))
    kyc_data_df = kyc_data_df.withColumn("ACQUISITION_DATE", to_date(kyc_data_df["ACQUISITION_DATE"], "yyyy-MM-dd"))
    
    # Calculate age and tenure for filtering
    kyc_data_df = kyc_data_df.withColumn("age", floor(datediff(current_date(), col("BIRTH_DATE")) / 365.25) )
    kyc_data_df = kyc_data_df.withColumn("tenure_years", floor(datediff(current_date(), col("ACQUISITION_DATE")) / 365.25))

    # Merge the datasets
    merged_data = user_data_df.join(
        kyc_data_df, 
        on="SIM_NUMBER",  
        how="inner"   
    )
    merged_data.write \
        .format("mongodb") \
        .option("database", load_params.spark_config["database"]) \
        .option("collection", "merged_data") \
        .mode("overwrite") \
        .save()
    return f"Fichier fusionné sauvegardé dans la collection merged_data" 
# Code exécuté directement si le script est appelé
if __name__ == "__main__":
    load_and_merge_data(None, None)