from kafka import KafkaConsumer
import json

# Define the Kafka topic
confirmed_order_topic = "order_confirmed"

# Set up the Kafka consumer
consumer = KafkaConsumer(
    confirmed_order_topic,
    bootstrap_servers="localhost:29092"
)

# Initialize counters for orders and revenue
order_count = 0
revenue = 0.0

print("Starting analytics processing...")

# Start consuming messages from Kafka
while True:
    for msg in consumer:
        print("Updating analytics data...")
        message_data = json.loads(msg.value.decode())
        order_total = float(message_data["total_cost"])
        
        # Update total orders and revenue
        order_count += 1
        revenue += order_total
        
        # Display the ongoing count of orders and revenue
        print(f"Orders processed today: {order_count}")
        print(f"Total revenue generated: {revenue}")
