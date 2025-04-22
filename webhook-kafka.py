from kafka import KafkaConsumer
import ssl
import requests
import json

# SSL setup
ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ssl_context.load_cert_chain(
    certfile='certs/client.crt',
    keyfile='certs/client.key'
)
ssl_context.load_verify_locations('certs/ca.crt')

# Kafka consumer
consumer = KafkaConsumer(
    'aap-filtering',
    bootstrap_servers='hammer-kafka-kafka-tls-bootstrap-kafka-eda.apps.hammer-sno.arsalan.io:443',
    security_protocol='SSL',
    ssl_context=ssl_context,
    group_id='my-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
)

# Webhook settings
webhook_url = 'https://aap-25-demos-aap-25.apps.hammer-sno.arsalan.io/eda-event-streams/api/eda/v1/external_event_stream/518c3565-891b-4623-b27d-4eadf30a11f3/post/'
auth_token = 'AA1BB2CC3ZZ9YY8XX7'

headers = {
    'Authorization': f'Bearer {auth_token}',
    'Content-Type': 'application/json',
}

for message in consumer:
    print(f"Received message: {message.value}")

    try:
        payload = {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "key": message.key.decode('utf-8') if message.key else None,
            "value": message.value.decode('utf-8'),
        }

        response = requests.post(webhook_url, json=payload, headers=headers)
        response.raise_for_status()

        print(f"Webhook called successfully: {response.status_code}")
    except Exception as e:
        print(f"Failed to call webhook: {e}")

