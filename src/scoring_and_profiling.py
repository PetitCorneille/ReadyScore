from src.scoring_functions import (
    calculate_mobile_money_scores,
    calculate_data_service_scores,
    calculate_voice_service_scores,
    calculate_sms_service_scores,
    calculate_digital_service_scores,
    generate_profile_code
)

def calculate_all_scores(filtered_data):
    """
    Calcule les scores pour tous les services (Mobile Money, Data, Voice, SMS, Digital)
    et met à jour la DataFrame.

    Args:
        filtered_data (pd.DataFrame): Données pré-filtrées.

    Returns:
        pd.DataFrame: Données enrichies avec les scores de tous les services.
    """
    print("Calcul des scores Mobile Money...")
    filtered_data = calculate_mobile_money_scores(filtered_data)

    print("Calcul des scores Data Service...")
    filtered_data = calculate_data_service_scores(filtered_data)

    print("Calcul des scores Voice Service...")
    filtered_data = calculate_voice_service_scores(filtered_data)

    print("Calcul des scores SMS Service...")
    filtered_data = calculate_sms_service_scores(filtered_data)

    print("Calcul des scores Digital Service...")
    filtered_data = calculate_digital_service_scores(filtered_data)

    return filtered_data

def verify_scoring_columns(filtered_data, required_scoring_columns):
    """
    Vérifie si les colonnes de scoring nécessaires sont présentes dans la DataFrame.

    Args:
        filtered_data (pd.DataFrame): DataFrame contenant les données enrichies.
        required_scoring_columns (list): Liste des colonnes de scoring nécessaires.

    Raises:
        ValueError: Si des colonnes de scoring sont manquantes.

    Returns:
        None
    """
    print("Vérification des colonnes de scoring...")
    missing_columns = [col for col in required_scoring_columns if col not in filtered_data.columns]

    if missing_columns:
        print(f"Erreur : Les colonnes de scoring suivantes sont manquantes : {missing_columns}")
        raise ValueError("Colonnes de scoring manquantes. Vérifiez les étapes précédentes.")
    else:
        print("Toutes les colonnes de scoring nécessaires sont présentes :")
        print(required_scoring_columns)

def verify_profile_code(scored_data):
    """
    Vérifie si la colonne 'Profile_Code' est présente dans la DataFrame
    et affiche un exemple des valeurs.

    Args:
        scored_data (pd.DataFrame): DataFrame contenant les données scorées.

    Raises:
        ValueError: Si la colonne 'Profile_Code' est manquante.

    Returns:
        None
    """
    print("Vérification de la colonne Profile_Code...")
    if 'Profile_Code' in scored_data.columns:
        print("La colonne 'Profile_Code' a été créée avec succès.")
        print("Exemples des valeurs de Profile_Code :")
        print(scored_data[[
            'SIM_NUMBER', 'Mobile_Money_Score', 'Data_Service_Score', 
            'Voice_Service_Score', 'SMS_Service_Score', 
            'Digital_Service_Score', 'Profile_Code'
        ]].head())
    else:
        print("Erreur : La colonne 'Profile_Code' n'a pas été créée.")
        raise ValueError("La colonne 'Profile_Code' est manquante dans scored_data.")
    
    
if __name__ == "__main__":
    print("Ne s\'utilise pas ainsi...")