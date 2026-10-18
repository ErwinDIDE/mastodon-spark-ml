import json
import time
from mastodon import Mastodon
from kafka import KafkaProducer

# Configuration de Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC_NAME = 'mastodon_stream'

# Connexion à l'API Mastodon
mastodon = Mastodon(
    access_token='tQqS0E5m7X-AU4cWkPNi5lcWsArHPOhYmCrusVSCVeo',
    api_base_url='https://mastodon.social'
)

print("Démarrage du producteur Mastodon multi-hashtags...")

seen_ids = set()
# Liste des hashtags à surveiller
HASHTAGS = ['BigData', 'DataScience', 'AI', 'Python', 'MachineLearning']

while True:
    for hashtag in HASHTAGS:
        try:
            statuses = mastodon.timeline_hashtag(hashtag, limit=10)
            
            for status in statuses:
                status_id = str(status['id'])
                if status_id not in seen_ids:
                    seen_ids.add(status_id)
                    
                    if len(seen_ids) > 2000:
                        seen_ids.pop()
                    
                    toot_data = {
                        'id': status_id,
                        'timestamp': str(status['created_at']),
                        'text': status['content'],
                        'user_id': str(status['account']['id']),
                        'favourites_count': status['favourites_count'],
                        'reblogs_count': status['reblogs_count'],
                        'hashtag': hashtag  # On ajoute le hashtag pour le suivi
                    }
                    
                    producer.send(TOPIC_NAME, value=toot_data)
                    print(f"Toot (#{hashtag}) envoyé : {toot_data['id']}")
                    
            # Petite pause entre chaque hashtag pour respecter les quotas
            time.sleep(5)
            
        except Exception as e:
            print(f"Erreur sur le hashtag #{hashtag} : {e}")
            
    # Pause globale avant de relancer un tour complet sur tous les hashtags
    print("--- Fin du cycle, pause de 15 secondes ---")
    time.sleep(15)