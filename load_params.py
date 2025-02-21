import json

def charger_json(fichier):
    global spark_config, country_codes,individual_period, corporates_period,operator_patterns,\
    refund_period,server_ips,tranche_age
    
    with open(fichier, "r") as f:
        config = json.load(f)

    # Définition des variables globales
    spark_config      = config["spark"]
    country_codes     = config["country_codes"]
    individual_period = config["individual_period"]
    corporates_period = config["corporates_period"]
    operator_patterns = config["operator_patterns"]
    refund_period     = config["refund_period"]
    server_ips        = config["server_ips"]
    tranche_age       = config["tranche_age"]
