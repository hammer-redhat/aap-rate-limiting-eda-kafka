from kafka import KafkaConsumer
import ssl

# SSL context
ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ssl_context.load_cert_chain(
    certfile='certs/client.crt',
    keyfile='certs/client.key'
)
ssl_context.load_verify_locations('certs/ca.crt')  # Optional but recommended

consumer = KafkaConsumer(
    'aap-filtering',
    bootstrap_servers='hammer-kafka-kafka-tls-bootstrap-kafka-eda.apps.hammer-sno.arsalan.io:443',
    security_protocol='SSL',
    ssl_context=ssl_context,
    group_id='my-group',
    auto_offset_reset='earliest',  # start from the beginning
    enable_auto_commit=True,
)

for message in consumer:
    print(f"{message.topic}:{message.partition}:{message.offset}: key={message.key} value={message.value}")

