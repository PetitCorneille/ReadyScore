from pymongo import MongoClient
from confluent_kafka  import Producer
import json

# Connecter à MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['scoring']
collection = db['loans_repayment']

# Configurer le producteur Kafka
kafka_conf = {'bootstrap_servers':'localhost:9092'}
producer = Producer(kafka_conf)

# Fonction pour envoyer les messages à Kafka
def send_to_kafka(change):
    data = change["fullDocument"]  # Récupère le document inséré
    producer.produce("mongo_topic", key=str(data["_id"]), value=json.dumps(data, default=str))
    producer.flush()
    print(f"Message envoyé : {data}")

# Écouter les nouvelles insertions
print("En attente d'inserts dans MongoDB...")
with collection.watch([{"$match": {"operationType": "insert"}}]) as stream:
    for change in stream:
        send_to_kafka(change)