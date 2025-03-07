import re
from pyspark.sql.functions import udf,array, lit
from pyspark.sql.types import StringType
import load_params
import rstr
import random
import time
import pdb

def ajout_colonne (df,spark):
    #load_params.charger_json("config/config.json")
    operator_patterns = load_params.operator_patterns
    country_codes =  load_params.country_codes
    server_ips = load_params.server_ips
    
    def generate_phone_number(n, *regex_list):
        if not regex_list:
            raise ValueError("Veuillez fournir au moins une regex.")
        numbers = []
        num_regex = len(regex_list)
        n_per_regex = n // num_regex  # Nombre de numéros par regex
        remaining = n % num_regex  # Pour gérer les cas où `n` n'est pas divisible

        for i, regex in enumerate(regex_list):
            count = n_per_regex + (1 if i < remaining else 0)  # Répartir les restes équitablement
            for _ in range(count):
                numbers.append(rstr.xeger(regex))

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
    phone = generate_phone_number(n,*operator_patterns.values())
    # Définition de l'UDF
    detect_country_udf = udf(detect_country, StringType())
    get_server_ip_udf = udf(get_server_ip, StringType())
    # Ajout des colonnes Opérateur, Pays et IP Serveur
    #if len(phone) < n:
    #    phone = phone * (n // len(phone)) + phone[:n % len(phone)]
    random.shuffle(phone)
    df_phone = spark.createDataFrame([(phone,) for phone in phone], ["TELEPHONE"])
    df_indexe = df.rdd.zipWithIndex().map(lambda x: (x[1],) + x[0]).toDF(["index"] + df.columns)
    df_phone_indexe = df_phone.rdd.zipWithIndex().map(lambda x: (x[1],) + x[0]).toDF(["index", "TELEPHONE"])
    df = df_indexe.join(df_phone_indexe, on="index").drop("index")
    #df = df.withColumn("Pays", detect_country_udf(df["SIM_NUMBER"]))
    #df = df.withColumn("IP_Server", get_server_ip_udf(df["Operateur_pays"]))

    return df