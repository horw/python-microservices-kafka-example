from kafka import KafkaConsumer
import json

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:9092"
)

total_order_count = 0
total_revenue = 0

print("Other Service is listening...")
while True:
    for message in consumer:
        print("Update data")
        consumer_message = json.loads(message.value.decode())
        print(consumer_message)

        total_cost = float(consumer_message["total_cost"])
        total_revenue += total_cost
        total_order_count += 1

        print(f"Order so far today: {total_order_count}")
        print(f"Revenue so far today: {total_revenue}")