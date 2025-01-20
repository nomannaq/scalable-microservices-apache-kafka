import json
import time
from kafka import KafkaProducer

# Configuration
topic = "order_details"
order_count_limit = 20000

# Kafka producer setup
kafka_producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Orders will start generating in 5 seconds.")
print("One unique order will be generated every 5 seconds.")

# Loop to generate and send orders
for order_number in range(1, order_count_limit):
    order_data = {
        "order_id": order_number,
        "user_id": f"tom_{order_number}",
        "total_cost": order_number * 2,
        "items": "burger, sandwich",
    }

    kafka_producer.send(
        topic,
        json.dumps(order_data).encode("utf-8")
    )

    # Output for each order sent
    print(f"Order {order_number} sent.")

# Final message and flush
print(f"Order generation completed. Total orders sent: {order_number}")
kafka_producer.flush()  # Ensure all messages are sent
