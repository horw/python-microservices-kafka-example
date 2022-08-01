from kafka import KafkaConsumer
import json

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:9092"
)

emails_sent_so_far = set()
print("Email is listening...")

while True:
    for message in consumer:
        consumer_message = json.loads(message.value.decode())
        customer_email = consumer_message["customer_email"]
        print(f"Sending email to {customer_email}")
        emails_sent_so_far.add(customer_email)
        print(f"So far email sent to {len(emails_sent_so_far)} unique emails")
