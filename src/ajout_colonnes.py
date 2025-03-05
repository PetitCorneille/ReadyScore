import re
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import StringType
import load_params
import random
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
        
        # Fonction pour extraire les préfixes d'un regex donné
        def extract_prefixes(regex):
            match = re.search(r"\^237(\d{2})", regex)  # Cherche les préfixes après "237"
            if match:
                return match.group(1)
            return None

        # Générer n_regex1 numéros pour regex1
        for _ in range(n_regex1):
            while True:
                number = "237"  # Préfixe fixe
                prefix1 = extract_prefixes(regex1)  # Extraire le préfixe dynamique du regex1
                
                if prefix1:
                    # Générer un numéro avec le préfixe extrait
                    number += prefix1 + "".join([str(random.randint(0, 9)) for _ in range(7)])
                    if re.match(regex1, number):  # Valider avec le regex1
                        numbers.append(number)
                        break

        # Générer n_regex2 numéros pour regex2
        for _ in range(n_regex2):
            while True:
                number = "237"  # Préfixe fixe
                prefix2 = extract_prefixes(regex2)  # Extraire le préfixe dynamique du regex2
                
                if prefix2:
                    # Générer un numéro avec le préfixe extrait
                    number += prefix2 + "".join([str(random.randint(0, 9)) for _ in range(7)])
                    if re.match(regex2, number):  # Valider avec le regex2
                        numbers.append(number)
                        break

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
    if len(phone) < n:
        phone = phone * (n // len(phone)) + phone[:n % len(phone)]

    df = df.withColumn("TELEPHONE", lit(phone[0]))
    df = df.withColumn("Pays", detect_country_udf(df["SIM_NUMBER"]))
    df = df.withColumn("IP_Server", get_server_ip_udf(df["Operateur_pays"]))

    return df