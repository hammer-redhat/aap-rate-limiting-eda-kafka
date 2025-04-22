from kafka import KafkaProducer
import ssl

# Paths to your cert and key files
ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ssl_context.load_verify_locations('certs/ca.crt')
ssl_context.load_cert_chain(
    certfile='certs/client.crt',
    keyfile='certs/client.key'
)

producer = KafkaProducer(
    bootstrap_servers='hammer-kafka-kafka-tls-bootstrap-kafka-eda.apps.hammer-sno.arsalan.io:443',
    security_protocol='SSL',
    ssl_context=ssl_context,
)

producer.send('aap-filtering', b'job_id: 55')
producer.flush()
