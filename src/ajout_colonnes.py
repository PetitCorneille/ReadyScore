import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import load_params
import pdb

def ajout_colonne (df):
    #load_params.charger_json("config/config.json")
    operator_patterns = load_params.operator_patterns
    country_codes =  load_params.country_codes
    server_ips = load_params.server_ips
    
    def detect_operator(phone_number):
        for operator, pattern in operator_patterns.items():
            if re.match(pattern, phone_number):
                return operator
            else :
                return None
        return "Inconnu"
    
    def detect_country(phone_number):
        for code, country in country_codes.items():
            if phone_number.startswith(code):
                return country
            else :
                return None
        return "Inconnu"

    def get_server_ip(operator):
        for pays, ip in server_ips.items():
            if pays == operator:
                return ip
            else :
                return None
        return "Inconnu"
    
    # Définition de l'UDF
    detect_operator_udf = udf(detect_operator, StringType())
    detect_country_udf = udf(detect_country, StringType())
    get_server_ip_udf = udf(get_server_ip, StringType())
    # Ajout des colonnes Opérateur, Pays et IP Serveur
   
    df = df.withColumn("Operateur_pays", detect_operator_udf(df["SIM_NUMBER"]))
    df = df.withColumn("Pays", detect_country_udf(df["SIM_NUMBER"]))
    df = df.withColumn("IP_Server", get_server_ip_udf(df["Operateur_pays"]))

    return df