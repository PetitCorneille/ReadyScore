import re
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import StringType
import load_params
import rstr
import random
import time
import pdb

def ajout_colonne (df):
    #load_params.charger_json("config/config.json")
    operator_patterns = load_params.operator_patterns
    regex = list(operator_patterns.items())
    country_codes =  load_params.country_codes
    server_ips = load_params.server_ips
    
    def generate_phone_number(regex1, regex2, n):
        numbers = []
        # Générer la moitié des numéros pour regex1 et l'autre moitié pour regex2
        n_regex1 = n // 2
        n_regex2 = n - n_regex1  # Pour s'assurer qu'on génère le bon nombre de numéros au total
        
        regex1_mod = regex1.lstrip("^").rstrip("$")
        regex2_mod = regex2.lstrip("^").rstrip("$")

        # Génération pour regex1
        for i in range(n_regex1):
            # Modifier la graine à chaque itération en combinant le temps et l'indice
            random.seed(time.time() + i)
            numbers.append(rstr.xeger(regex1_mod))
        
        # Génération pour regex2
        for i in range(n_regex2):
            random.seed(time.time() + i)
            numbers.append(rstr.xeger(regex2_mod))

        return numbers
    
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
    n = df.count()
    phone = generate_phone_number(regex[0][1], regex[1][1], n)
    # Définition de l'UDF
    detect_country_udf = udf(detect_country, StringType())
    get_server_ip_udf = udf(get_server_ip, StringType())
    # Ajout des colonnes Opérateur, Pays et IP Serveur
    #if len(phone) < n:
    #    phone = phone * (n // len(phone)) + phone[:n % len(phone)]

    df = df.withColumn("TELEPHONE", lit(phone))
    #df = df.withColumn("Pays", detect_country_udf(df["SIM_NUMBER"]))
    #df = df.withColumn("IP_Server", get_server_ip_udf(df["Operateur_pays"]))

    return df