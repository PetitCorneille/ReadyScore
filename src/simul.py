import os
import pandas as pd
import numpy as np
from faker import Faker
import random
from pymongo import MongoClient
import gc

# Initialiser Faker pour générer des données aléatoires
fake = Faker()

def simulate_data(output_path):
    """
    Simule les données de transactions, d'utilisateur (USER_DATA) et de KYC.

    Args:
        output_path (str): Chemin du répertoire où les fichiers simulés seront sauvegardés.

    Returns:
        list: Liste des chemins des fichiers générés.
    """
    # Assurez-vous que le chemin de sortie existe
    os.makedirs(output_path, exist_ok=True)

    ####################################### Transactions Data ######################################
    print("Simulation des données de transactions...")
    
    # Charger le dataset réel (simulé ici avec des sous-échantillons)
    transactions_path = os.path.join(output_path, "PS_20174392719_1491204439457_log.csv")
    chunksize = 1000000
    n = 0
    client = MongoClient('mongodb://localhost:27017')
    db = client['scoring']
    user_kyc  = db['simulated_KYC_DATA2']
    user_dat  = db['simulated_USER_DATA_with_dates2']
    user_real = db['real_transactions_with_dates']
    user_kyc.delete_many({})
    user_dat.delete_many({})
    user_real.delete_many({})
    for transactions_df in pd.read_csv(transactions_path, chunksize=250000):
        #transactions_df = pd.read_csv(transactions_path).sample(n=1000000, random_state=123)
        n = n + 1
        batch_size = 200000
        print("le nombre de tour est: ", n)
        # Ajouter une colonne transaction_date basée sur 'step'
        start_date = pd.to_datetime('2023-01-01')
        transactions_df['transaction_date'] = start_date + pd.to_timedelta(transactions_df['step'], unit='d')
        
        # Sauvegarder le dataset de transactions avec les nouvelles dates
        data_dict = transactions_df.to_dict(orient="records")
        for i in range(0, len(data_dict), batch_size):
            batch = data_dict[i:i + batch_size]
            user_real.insert_many(batch)
        """  
        transactions_with_dates_csv = os.path.join(output_path, "real_transactions_with_dates.csv")
        transactions_with_dates_parquet = os.path.join(output_path, "real_transactions_with_dates.parquet")
        transactions_df.to_csv(transactions_with_dates_csv, index=False)
        transactions_df.to_parquet(transactions_with_dates_parquet, index=False)
        """

        # Extraire les SIM uniques (nameOrig et nameDest)
        #unique_customers = pd.unique(transactions_df[['nameOrig', 'nameDest']].values.ravel('K'))
        '''L\'ancien code cree des doublons'''
        unique_customers = pd.unique(transactions_df['nameOrig'].values.ravel('K'))

        ######################################### USER_DATA ###########################################
        print("Simulation des données utilisateur (USER_DATA)...")
        
        # Nombre total de clients uniques
        num_customers = len(unique_customers)
        print("le nombre de lot est : ", num_customers)
        # Générer les données USER_DATA
        user_data = pd.DataFrame({
            'DATE_OF_THE_DAY': np.random.choice(transactions_df['transaction_date'].dt.strftime('%Y%m%d'), size=num_customers),
            'SIM_NUMBER': unique_customers,
            'HAS_USED_MOB_MONEY_IN_LAST_30_DAYS': np.random.randint(0, 2, size=num_customers),
            'HAS_USED_MOB_MONEY_IN_LAST_7_DAYS': np.random.randint(0, 2, size=num_customers),
            'HAS_USED_MOB_MONEY_IN_LAST_90_DAYS': np.random.randint(0, 2, size=num_customers),
            'PAID_DATA_VOLUME': np.random.uniform(0, 5000, size=num_customers).round(2),
            'PAID_VOICE_TRAFFIC': np.random.uniform(0, 2000, size=num_customers).round(2),
            'HAS_USED_MOB_MONEY_IN_LAST_1_DAY': np.random.randint(0, 2, size=num_customers),
            'VOICE_REVENUE': np.random.uniform(0, 500, size=num_customers).round(2),
            'DATA_REVENUE': np.random.uniform(0, 300, size=num_customers).round(2),
            'SMS_REVENUE': np.random.uniform(0, 100, size=num_customers).round(2),
            'DIGITAL_REVENUE': np.random.uniform(0, 200, size=num_customers).round(2),
            'MOB_MONEY_REVENUE': np.random.uniform(0, 1000, size=num_customers).round(2),
            'FREE_VOICE_TRAFFIC': np.random.uniform(0, 1000, size=num_customers).round(2),
            'FREE_DATA_VOLUME': np.random.uniform(0, 10000, size=num_customers).round(2),
            'VOICE_TRAFFIC_ONNET': np.random.uniform(0, 500, size=num_customers).round(2),
            'VOICE_TRAFFIC_OFFNET': np.random.uniform(0, 500, size=num_customers).round(2),
            'NB_CALLS_EMITTED_ONNET': np.random.randint(0, 50, size=num_customers),
            'NB_CALLS_RECEIVED_ONNET': np.random.randint(0, 50, size=num_customers),
            'NB_CALLS_EMITTED_OFFNET': np.random.randint(0, 50, size=num_customers),
            'NB_CALLS_RECEIVED_OFFNET': np.random.randint(0, 50, size=num_customers),
            'IS_RGS_1': np.random.randint(0, 2, size=num_customers),
            'IS_RGS_7': np.random.randint(0, 2, size=num_customers),
            'IS_RGS_30': np.random.randint(0, 2, size=num_customers),
            'IS_RGS_90': np.random.randint(0, 2, size=num_customers),
            'NB_CALLS_EMITTED_INTERNATIONAL': np.random.randint(0, 10, size=num_customers),
            'NB_CALLS_RECEIVED_INTERNATIONAL': np.random.randint(0, 10, size=num_customers),
            'VOICE_OUTGOING_TRAFFIC_INTERNATIONAL': np.random.uniform(0, 200, size=num_customers).round(2),
            'VOICE_INCOMING_TRAFFIC_INTERNATIONAL': np.random.uniform(0, 200, size=num_customers).round(2),
            'VOICE_OUTGOING_TRAFFIC_ONNET': np.random.uniform(0, 500, size=num_customers).round(2),
            'VOICE_INCOMING_TRAFFIC_ONNET': np.random.uniform(0, 500, size=num_customers).round(2),
            'VOICE_OUTGOING_TRAFFIC_OFFNET': np.random.uniform(0, 500, size=num_customers).round(2),
            'VOICE_INCOMING_TRAFFIC_OFFNET': np.random.uniform(0, 500, size=num_customers).round(2),
            'NB_VOICE_PACKAGES_SUBSCRIPTIONS': np.random.randint(0, 10, size=num_customers),
            'NB_DATA_PACKAGES_SUBSCRIPTIONS': np.random.randint(0, 10, size=num_customers),
            'VOICE_PACKAGES_REVENUE': np.random.uniform(0, 200, size=num_customers).round(2),
            'NB_SMS_SENT_ONNET': np.random.randint(0, 50, size=num_customers),
            'NB_SMS_SENT_OFFNET': np.random.randint(0, 50, size=num_customers),
            'NB_SMS_RECEIVED_ONNET': np.random.randint(0, 50, size=num_customers),
            'NB_SMS_RECEIVED_OFFNET': np.random.randint(0, 50, size=num_customers),
            'NB_SMS_SENT_INTERNATIONAL': np.random.randint(0, 10, size=num_customers),
            'NB_SMS_RECEIVED_INTERNATIONAL': np.random.randint(0, 10, size=num_customers),
            'NB_SMS_PACKAGES_SUBSCRIPTIONS': np.random.randint(0, 10, size=num_customers),
            'SMS_PACKAGE_REVENUE': np.random.uniform(0, 100, size=num_customers).round(2),
            'NB_VOICE_PACKAGES_SUBS_VIA_MOB_MONEY': np.random.randint(0, 5, size=num_customers),
            'NB_VOICE_PACKAGES_SUBS_VIA_POS': np.random.randint(0, 5, size=num_customers),
            'NB_VOICE_PACKAGES_SUBS_VIA_MAIN_ACCOUNT': np.random.randint(0, 5, size=num_customers),
            'NB_DATA_package_SUBS_VIA_MOB_MONEY': np.random.randint(0, 5, size=num_customers),
            'NB_DATA_package_SUBS_VIA_POS': np.random.randint(0, 5, size=num_customers),
            'NB_DATA_package_SUBS_VIA_MAIN_ACCOUNT': np.random.randint(0, 5, size=num_customers),
            'NB_SMS_package_SUBS_VIA_MOB_MONEY': np.random.randint(0, 5, size=num_customers),
            'NB_SMS_package_SUBS_VIA_POS': np.random.randint(0, 5, size=num_customers),
            'NB_SMS_package_SUBS_VIA_MAIN_ACCOUNT': np.random.randint(0, 5, size=num_customers),
            'NB_MIXED_package_SUBS_VIA_MOB_MONEY': np.random.randint(0, 5, size=num_customers),
            'NB_MIXED_package_SUBS_VIA_POS': np.random.randint(0, 5, size=num_customers),
            'NB_MIXED_package_SUBS_VIA_MAIN_ACCOUNT': np.random.randint(0, 5, size=num_customers),
            'IS_SMARTPHONE_USER': np.random.randint(0, 2, size=num_customers),
            'LAST_EVENT_DATE': pd.to_datetime('today') - pd.to_timedelta(np.random.randint(1, 365, size=num_customers), unit='d'),
            'LAST_EVENT_TYPE': np.random.choice(['Payment', 'Transfer', 'Cash_Out', 'Debit'], size=num_customers),
            'IS_DATA_RGS1': np.random.randint(0, 2, size=num_customers),
            'IS_DATA_RGS7': np.random.randint(0, 2, size=num_customers),
            'IS_DATA_RGS30': np.random.randint(0, 2, size=num_customers),
            'IS_DATA_RGS90': np.random.randint(0, 2, size=num_customers),
            'MAIN_ACCOUNT_AMOUNT': np.random.uniform(0, 10000, size=num_customers).round(2),
            'EXTRA_TIME_LOAN_AMOUNT_RENT': np.random.uniform(0, 500, size=num_customers).round(2),
            'EXTRA_TIME_LOAN_AMOUNT_TO_PAY_BACK': np.random.uniform(0, 500, size=num_customers).round(2),
            'REFILL_MAIN_AMOUNT': np.random.uniform(0, 1000, size=num_customers).round(2),
            'REFILL_mobile_money_ACCOUNT': np.random.uniform(0, 1000, size=num_customers).round(2),
            'TOTAL_SPENT_MAIN_ACCOUNT': np.random.uniform(0, 5000, size=num_customers).round(2),
            'TOTAL_SPENT_MOB_MONEY_ACCOUNT': np.random.uniform(0, 5000, size=num_customers).round(2),
            'NB_package_GIFTS_SENT': np.random.randint(0, 10, size=num_customers),
            'NB_package_GIFTS_RECEIVED': np.random.randint(0, 10, size=num_customers),
            'AMOUNT_package_GIFTS_SENT': np.random.uniform(0, 1000, size=num_customers).round(2),
            'AMOUNT_package_GIFTS_RECEIVED': np.random.uniform(0, 1000, size=num_customers).round(2),
            'MOB_MONEY_ACCOUNT_AMOUNT': np.random.uniform(0, 10000, size=num_customers).round(2),
            'TOTAL_LOADING_MONEY_IN_MOB_MONEY': np.random.uniform(0, 5000, size=num_customers).round(2),
            'TOTAL_CASHOUT_MOB_MONEY_ACCOUNT': np.random.uniform(0, 5000, size=num_customers).round(2),
            'TOTAL_CASHOUT_MOB_MONEY_FOR_package_PURCHASE': np.random.uniform(0, 2000, size=num_customers).round(2),
            'TOTAL_CASHOUT_MOB_MONEY_TRANSFER_MONEY': np.random.uniform(0, 3000, size=num_customers).round(2)
        })
        
        data_dict = user_data.to_dict(orient="records")
        for i in range(0, len(data_dict), batch_size):
            batch = data_dict[i:i + batch_size]
            user_dat.insert_many(batch)
        #collection_user_data.insert_many(data_u)
        del data_dict
        gc.collect()
        print("Insertion user data réussie !")
        #user_data_csv = os.path.join(output_path, "simulated_USER_DATA_with_dates.csv")
        #user_data_parquet = os.path.join(output_path, "simulated_USER_DATA_with_dates.parquet")
        #user_data.to_csv(user_data_csv, index=False)
        #user_data.to_parquet(user_data_parquet, index=False)

        ######################################### KYC Data ###########################################
        print("Simulation des données KYC...")
        
        # Générer les données KYC

        # Ensure consistent dates: Use transaction_date range from the Transactions Dataset
        transaction_dates = pd.to_datetime(transactions_df['transaction_date'])
        start_transaction_date = transaction_dates.min()
        end_transaction_date = transaction_dates.max()

        # Generate acquisition dates before first event dates, and first event dates within the transaction date range
        acquisition_dates = start_transaction_date - pd.to_timedelta(np.random.randint(30, 365, size=num_customers), unit='d')
        first_event_dates = np.random.choice(transaction_dates, size=num_customers)

        kyc_data = pd.DataFrame({
            'SIM_NUMBER': unique_customers,
            # Acquisition date happens before the first event date
            'ACQUISITION_DATE': acquisition_dates,
            'SIM_ACTIVATION_COMPLETION_TIME': np.random.randint(1, 48, size=num_customers),  # Time in hours
            'CUST_CATEGORY': np.random.choice(['Individual', 'Business'], size=num_customers), # Revoir en fonction du Montant dans son Mobile Money
            'REGION': [fake.state() for _ in range(num_customers)],
            'TOWN': [fake.city() for _ in range(num_customers)],
            'TERRITORY': [fake.state_abbr() for _ in range(num_customers)],
            # First event date should align with transaction dates, slightly before or after acquisition
            'FIRST_EVENT_DATE': first_event_dates,
            'FIRST_NAME': [fake.first_name() for _ in range(num_customers)],
            'LAST_NAME': [fake.last_name() for _ in range(num_customers)],
            'GENDER': np.random.choice(['Male', 'Female'], size=num_customers),
            'BIRTH_DATE': pd.to_datetime('today') - pd.to_timedelta(np.random.randint(18*365, 65*365, size=num_customers), unit='d'), # Majorité : 21 ans
            'NATIONALITY': [fake.country() for _ in range(num_customers)],
            'ID_TYPE': np.random.choice(['National ID', 'Passport', 'Driver\'s License'], size=num_customers),
            'ID_NUMBER': [fake.ssn() for _ in range(num_customers)],
            'ID_EXPIRY_DATE': pd.to_datetime('today') + pd.to_timedelta(np.random.randint(365, 3650, size=num_customers), unit='d'),
            'HAS_SIM_NUM_PARENT': np.random.randint(0, 2, size=num_customers),
            'REGISTRATION_STATUS': np.random.choice(['Accepted', 'Not Accepted'], size=num_customers),
            'REGISTRATION_TYPE': np.random.choice(['New', 'Existing'], size=num_customers),
            'LANG': np.random.choice(['French', 'English'], size=num_customers),
            'IS_MOB_MONEY_MERCHANT': np.random.randint(0, 2, size=num_customers),
            'IS_MOB_MONEY_USER': np.random.randint(0, 2, size=num_customers),
            'DEVICE_TYPE': np.random.choice(['Smartphone', 'Feature phone'], size=num_customers)
        })
        
        data_u = kyc_data.to_dict(orient="records")
        for i in range(0, len(data_u), batch_size):
            batch = data_u[i:i + batch_size]
            user_kyc.insert_many(batch)
        #collection_user_data.insert_many(data_u)
        del data_u
        gc.collect()

        print("Insertion user kyc réussie !")  
        #kyc_data_csv = os.path.join(output_path, "simulated_KYC_DATA.csv")
        #kyc_data_parquet = os.path.join(output_path, "simulated_KYC_DATA.parquet")
        #kyc_data.to_csv(kyc_data_csv, index=False)
        #kyc_data.to_parquet(kyc_data_parquet, index=False)

    print("Simulation terminée.")
        
        # Retourner les chemins des fichiers générés
    #return [transactions_with_dates_csv, transactions_with_dates_parquet, user_data_csv, user_data_parquet, kyc_data_csv, kyc_data_parquet]

# Exécutable pour tester indépendamment
if __name__ == "__main__":
    simulate_data("data/raw/")

