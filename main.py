
import pdb
from pyspark.sql import SparkSession
from src.bonus_malus_calculation import calculate_bonus_malus, process_transactions, update_loans_with_bonus_malus, update_transactions
from src.cash_allocation import calculate_business_credits, calculate_individual_credits
from src.multi_sim_management import manage_multi_sim_clients, validate_final_clients
from src.data_filtering import filter_data
from src.data_processing import load_and_merge_data
from src.scoring_and_profiling import calculate_all_scores
from src.scoring_functions import generate_profile_code
from src.segmentation import segment_profiles
from src.ajout_colonnes import ajout_colonne
from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import StructType, StructField, IntegerType
import load_params
import time

start_time = time.time()

#Lecture des données de config
load_params.charger_json("config/config.json")
spark = SparkSession.builder \
    .appName("MongoDB_Spark") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .config("spark.driver.memory", load_params.spark_config["memoire_driver"])\
    .config("spark.executor.memory", load_params.spark_config["memoire_executor"])\
    .master(load_params.spark_config["master"])\
    .config("spark.mongodb.read.connection.uri", load_params.spark_config["mongodb_uri_l"]) \
    .config("spark.executor.cores", load_params.spark_config["nombre_cores"]) \
    .config("spark.cores.max", load_params.spark_config["nombre_cores_max"]) \
    .getOrCreate()
""" 
print("Étape 0 : Ajout des colonnes aux data sets ....")
dfc = spark.read.format("mongodb") \
          .option("database", load_params.spark_config["database"]) \
          .option("collection", "simulated_KYC_DATA2") \
          .load()
dfu = spark.read.format("mongodb") \
          .option("database", load_params.spark_config["database"]) \
          .option("collection", "simulated_USER_DATA_with_dates2") \
          .load()
dfc = ajout_colonne(dfc)
print("Colonnes ajoutées aux user data")
dfu = ajout_colonne(dfu)
print("Colonnes ajoutées aux user kyc")

#dfc.delete_many({})

dfc.write \
    .format("mongodb") \
    .option("database", load_params.spark_config["database"] ) \
    .option("collection", "simulated_KYC_DATA") \
    .mode("overwrite") \
    .save() 
#dfu.delete_many({})

dfu.write \
    .format("mongodb") \
    .option("database", load_params.spark_config["database"]) \
    .option("collection", "simulated_USER_DATA_with_dates") \
    .mode("overwrite") \
    .save()  
# Étape 1 : Simulation des données
print("Étape 1 : Simulation des données...")
print("les données n'ont pas besoin d'être simulés on vas les lire en bd directement")
#colect = simulate_data(raw_data_path)
#print(f"Données simulées sauvegardées dans {colect}")

#Étape 2 : Fusion des données
print("Étape 2 : Fusion des données...")

data_sim = spark.read.format("mongodb") \
                .option("database", load_params.spark_config["database"]) \
                .option("collection", "simulated_USER_DATA_with_dates") \
                .load()
data_kyc = spark.read.format("mongodb") \
                .option("database", load_params.spark_config["database"]) \
                .option("collection", "simulated_KYC_DATA") \
                .load()
merged_data = load_and_merge_data(data_sim, data_kyc)
print(merged_data) 

# Étape 3 : 
print("Étape 3 : Veuillez exécuter le Notebook EDA dans notebooks/EDA_merged_data.ipynb.")

# Étape 4 : Pré-filtration des données clients
print("Étape 4 : Pré-filtration des données clients...")
data_merge = spark.read.format("mongodb") \
                .option("database", load_params.spark_config["database"]) \
                .option("collection", "merged_data") \
                .load()

filtered_data = filter_data(data_merge) 
#print(filtered_data)
# Étape 5 : Rappel pour l'EDA sur filtered_data
print("Étape 5 : Veuillez exécuter le Notebook EDA dans notebooks/EDA_filtered_data.ipynb.")

# Étape 6 : Scoring et génération des profils
try:
    print("Étape 6 : Calcul des scores et génération des profils...")
    filtered_data = calculate_all_scores(filtered_data)
    scored_data = generate_profile_code(filtered_data) 

    # Sauvegarde des données scorées
    scored_data.write \
        .format("mongodb") \
        .option("database", load_params.spark_config["database"]) \
        .option("collection", "scored_data") \
        .mode("overwrite") \
        .save() 
    
    print("Fichier scoré sauvegardé dans la collection scored_data")

except Exception as e:
    print(f"Erreur lors de l'étape 6 : {e}")
    print("Veuillez vérifier scoring_and_profiling.py pour diagnostiquer le problème.")
 
# Étape 7 : Analyse exploratoire des résultats de scoring et de profiling
print("Étape 7 : Veuillez exécuter le Notebook EDA dans notebooks/EDA_scored_data.ipynb.")
# Étape 8 : Segmentation des profils
print("Étape 8 : Segmentation des profils...")
try:
    scored_data = spark.read.format("mongodb") \
                .option("database", load_params.spark_config["database"]) \
                .option("collection", "scored_data") \
                .load()
    
    segmented_data = segment_profiles(scored_data)
    segmented_data.write \
        .format("mongodb") \
        .option("database", load_params.spark_config["database"]) \
        .option("collection", "segmented_data") \
        .mode("overwrite") \
        .save()
    print(f"Données segmentées sauvegardées dans la collection segmented_data")
except Exception as e:
    print(f"Erreur lors de l'étape 8 : {e}")
    print("Veuillez vérifier segmentation.py pour diagnostiquer le problème.") 

# Étape 9 : Analyse des segments
print("Étape 9 : Veuillez exécuter le Notebook EDA dans notebooks/EDA_segments.ipynb.")
print("Étape 10 : Attribution des crédits...")
try:
    segmented_data = spark.read.format("mongodb") \
                .option("database", load_params.spark_config["database"]) \
                .option("collection", "segmented_data") \
                .load()
    #segmented_data = pd.read_csv(segmented_data_path)

    # Appliquer les fonctions d'attribution
    output_schema = StructType([
        StructField("Nano_Loan", IntegerType(), True),
        StructField("Advanced_Credit", IntegerType(), True)
    ])
    
    calculate_individual_credits_udf = udf(calculate_individual_credits, output_schema)
    segmented_data = segmented_data.withColumn(
    "credit_struct", calculate_individual_credits_udf(struct(*segmented_data.columns)))

    segmented_data = segmented_data.withColumn("Nano_Loan", col("credit_struct.Nano_Loan")) \
                               .withColumn("Advanced_Credit", col("credit_struct.Advanced_Credit")) \
                               .drop("credit_struct")
    
    output_schem = StructType([
        StructField("Macro_Loan", IntegerType(), True),
        StructField("Cash_Roller_Over", IntegerType(), True)
    ])
    
    calculate_business_credits_udf = udf(calculate_business_credits, output_schem)
    segmented_data = segmented_data.withColumn(
    "credit", calculate_business_credits_udf(struct(*segmented_data.columns)))

    segmented_data = segmented_data.withColumn("Macro_Loan", col("credit.Macro_Loan")) \
                               .withColumn("Cash_Roller_Over", col("credit.Cash_Roller_Over")) \
                               .drop("credit")

    # Sauvegarder les résultats
    segmented_data.write \
        .format("mongodb") \
        .option("database", load_params.spark_config["database"]) \
        .option("collection", "cash_allocated_data") \
        .mode("overwrite") \
        .save()
        
    #pdb.set_trace()
    print(f"Données avec crédits sauvegardées dans la collection cash_allocated_data")
except Exception as e:
    print(f"Erreur lors de l'étape 10 : {e}")
    print("Veuillez vérifier cash_allocation.py pour diagnostiquer le problème.")
 
    # Étape 11 : Analyse des crédits alloués
print("Étape 11 : Veuillez exécuter le Notebook EDA dans notebooks/EDA_cash_allocated.ipynb.")
  """
# Étape 12 : Gestion des clients avec plusieurs SIM
print("Étape 12 : Gestion des clients avec plusieurs SIM...")
try:
    #cash_allocated_data = pd.read_csv(cash_allocated_data_path)
    
    cash_allocated_data = spark.read.format("mongodb") \
                .option("database", "scoring") \
                .option("collection", "cash_allocated_data") \
                .load()
    final_clients = manage_multi_sim_clients(cash_allocated_data)
    validate_final_clients(final_clients)
    final_clients.write \
        .format("mongodb") \
        .option("database", load_params.spark_config["database"]) \
        .option("collection", "final_clients") \
        .mode("overwrite") \
        .save()
    #final_clients.to_csv(final_clients_path, index=False)
    print(f"Données finales sauvegardées dans la collection final_clients")
except Exception as e:
    print(f"Erreur lors de l'étape 12 : {e}")
    print("Veuillez vérifier multi_sim_management.py pour diagnostiquer le problème.") 

# Étape 13 : Implémentation des bonus/malus
try:
    print("Étape 13 : Implémentation des bonus/malus...")

    # Étape a: Créer le DataFrame des transactions pour le mois précédent
    print("Traitement des transactions pour le mois précédent...")
    real_transactions_with_dates = spark.read.format("mongodb") \
            .option("database", load_params.spark_config["database"]) \
            .option("collection", "real_transactions_with_dates") \
            .load()

    transactions_previous_month = process_transactions(real_transactions_with_dates)

    # Étape b: Mise à jour des transactions avec les informations des clients finaux
    print("Mise à jour des transactions avec les données des clients finaux...")
    transactions_previous_month = update_transactions( transactions_previous_month,final_clients)
    transactions_previous_month.write \
                    .format("mongodb") \
                    .option("database", load_params.spark_config["database"]) \
                    .option("collection", "transactions_previous_month") \
                    .mode("overwrite") \
                    .save()
    
    # Étape c: Calcul des bonus/malus
    print("Calcul des bonus/malus pour les clients...")
    final_clients_with_bonus_malus = calculate_bonus_malus(transactions_previous_month, final_clients)

    # Étape d: Mise à jour des prêts des clients avec le bonus/malus
    print("Mise à jour des prêts des clients avec le bonus/malus...")
    final_clients_with_bonus_malus = update_loans_with_bonus_malus(final_clients_with_bonus_malus)
    final_clients_with_bonus_malus.write \
                    .format("mongodb") \
                    .option("database", load_params.spark_config["database"]) \
                    .option("collection", "final_clients_with_bonus_malus") \
                    .mode("overwrite") \
                    .save()
    print(f"Collections mise à jour avec succès dans la base de données Scoring.")

except Exception as e:
    print(f"Erreur lors de l'étape 13 : {e}")

    # Étape 14 : Rappel pour l'analyse exploratoire des Bonus/Malus et des Montants de Crédits Mis à Jour
print("Étape 14 : Veuillez exécuter le Notebook EDA dans notebooks/EDA_bonus_malus_updated_loans.ipynb.")
print("Pipeline exécuté avec succès !")
spark.stop()
end_time = time.time()  # Arrêter le chronomètre
execution_time = end_time - start_time
print(f"Temps d'exécution : {execution_time:.4f} secondes")