import pdb
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType



def segment_profiles(scored_data):
    """
    Segmente les profils en cinq catégories (Very High, High, Medium, Low, Very Low)
    en fonction du score pondéré calculé à partir de Profile_Code.

    Args:
        scored_data (pd.DataFrame): Données scorées contenant la colonne 'Profile_Code'.

    Returns:
        pd.DataFrame: Données enrichies avec les colonnes 'Weighted_Score' et 'Segment'.
    """
    def calculate_weighted_score(profile_code):
        if profile_code is None:
            return None
    
        profile_str = str(profile_code)
        if len(profile_str) != 5 or not profile_str.isdigit():
            return None
        weights = {
            'Mobile Money': 5,
            'Data': 4,
            'Voice': 3,
            'SMS': 2,
            'Digital': 1
        }
        # Calculer le score pondéré pour chaque Profile_Code
        profile_str = str(profile_code)

        mobile_money_score = int(profile_str[0]) * weights['Mobile Money']
        data_score = int(profile_str[1]) * weights['Data']
        voice_score = int(profile_str[2]) * weights['Voice']
        sms_score = int(profile_str[3]) * weights['SMS']
        digital_score = int(profile_str[4]) * weights['Digital']

        return float(mobile_money_score + data_score + voice_score + sms_score + digital_score)
    
    print("Calcul des scores pondérés...")
    # Définir la fonction UDF
    calculate_weighted_score_udf = F.udf(calculate_weighted_score, DoubleType())
    # Appliquer la UDF à la colonne Profile_Code
    scored_data = scored_data.withColumn("Weighted_Score", calculate_weighted_score_udf("Profile_Code"))
    
    # Calculer les percentiles pour la segmentation

    percentile_probs = [0.2, 0.4, 0.6, 0.8]
    relative_error = 0.01 

    # Calculer les percentiles pour chaque colonne
    percentiles = scored_data.approxQuantile('Weighted_Score', percentile_probs, relative_error)


    # Fonction pour catégoriser en fonction des percentiles
    def categorize_by_percentile(weighted_score):
        
        if weighted_score >= percentiles[3]:  # Au-dessus du 80e percentile
            return 'Very High'
        elif weighted_score >= percentiles[2]:  # Entre 60e et 80e percentile
            return 'High'
        elif weighted_score >= percentiles[1]:  # Entre 40e et 60e percentile
            return 'Medium'
        elif weighted_score >= percentiles[0]:  # Entre 20e et 40e percentile
            return 'Low'
        else:  # En dessous du 20e percentile
            return 'Very Low'

    print("Catégorisation des segments...")
    #Convertir la fonction en UDF
    categorize_by_percentile_udf = F.udf(categorize_by_percentile, StringType())
    # Apply the scoring function to the dataframe
    scored_data = scored_data.withColumn("Segment", categorize_by_percentile_udf("Weighted_Score"))
    #scored_data = scored_data.withColumn("Segment", categorize_by_percentile_udf(F.col("Weighted_Score")))
    return scored_data
if __name__ == "__main__":
    print("Ne s'appelle pas ainsi....")