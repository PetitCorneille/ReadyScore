
import pdb
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType


def calculate_individual(profile_code, category):
    """
    Calcule les crédits Nano et Advanced pour les clients individuels.

    Args:
        row (pd.Series): Ligne contenant les informations du client.

    Returns:
        tuple: (Nano Loan, Advanced Credit) pour les clients individuels.
    """

    # Only calculate for Individual clients
    if category != "Individual":
        return None, None  # Return None for non-Individual clients

    # Définir les poids pour chaque service
    weights = [5, 4, 3, 2, 1]  # Mobile Money, Data, Voice, SMS, Digital

    # Calculer le score pondéré
    weighted_score = sum(int(digit) * weight for digit, weight in zip(profile_code, weights))

    # Plage de score pondéré
    min_score, max_score = 15, 75

    # Define Nano Loan and Advanced Credit ranges
    nano_min, nano_max = 20, 45
    advanced_min, advanced_max = 100, 500

    # Scale weighted_score within the actual range (15 to 75)
    normalized_score = (weighted_score - min_score) / (max_score - min_score)

    # Calculate Nano Loan and Advanced Credit based on the normalized score
    nano_loan = nano_min + normalized_score * (nano_max - nano_min)
    advanced_credit = advanced_min + normalized_score * (advanced_max - advanced_min)
    return nano_loan, advanced_credit

# Définition d'une UDF (User Defined Function) pour Spark
def calculate_individual_credits_udf(profile_code, category):
    """
    UDF pour PySpark qui applique la logique de calcul des crédits.
    """
    nano_loan, advanced_credit = calculate_individual(profile_code, category)
    return (nano_loan, advanced_credit)


# Création de l'UDF Spark
calculate_individual_credits = udf(calculate_individual_credits_udf, StructType([
    StructField("Nano_Loan", DoubleType(), True),
    StructField("Advanced_Credit", DoubleType(), True)
]))

def calculate_business(profile_code, category):
    """
    Calcule les crédits Macro et Cash Roller Over pour les clients Business.

    Args:
        row (pd.Series): Ligne contenant les informations du client.

    Returns:
        tuple: (Macro Loan, Cash Roller Over) pour les clients Business.
    """
    # Only calculate for Business clients
    if category != "Business":
        return None, None  # Return None for non-Business clients

    # Définir les poids pour chaque service
    weights = [5, 4, 3, 2, 1]  # Mobile Money, Data, Voice, SMS, Digital

    # Calculer le score pondéré
    weighted_score = sum(int(digit) * weight for digit, weight in zip(profile_code, weights))

    # Plage de score pondéré
    min_score, max_score = 15, 75

    # Define Macro Loan and Cash Roller Over ranges
    macro_min, macro_max = 25, 250
    cash_roller_min, cash_roller_max = 100, 500

    # Scale weighted_score within the actual range (15 to 75)
    normalized_score = (weighted_score - min_score) / (max_score - min_score)

    # Calculate Macro Loan based on the normalized score
    macro_loan = macro_min + normalized_score * (macro_max - macro_min)
    cash_roller = cash_roller_min + normalized_score * (cash_roller_max - cash_roller_min)

    return macro_loan, cash_roller


def calculate_business_credits_udf(profile_code, category):
    """
    UDF pour PySpark qui applique la logique de calcul des crédits.
    """
    nano_loan, advanced_credit = calculate_business(profile_code, category)
    return (nano_loan, advanced_credit)

# Création de l'UDF Spark
calculate_business_credits = udf(calculate_business_credits_udf, StructType([
    StructField("Macro_Loan", DoubleType(), True),
    StructField("Cash_Roller_Over", DoubleType(), True)
]))
