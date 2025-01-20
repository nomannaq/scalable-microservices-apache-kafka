import json
from kafka import KafkaConsumer

# Define topic and consumer
topic = "order_confirmed"
kafka_consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:29092"
)

# Set to keep track of unique emails
sent_emails = set()

print("Listening for incoming transactions...")

# Start consuming messages
while True:
    for msg in kafka_consumer:
        message_data = json.loads(msg.value.decode())
        email_address = message_data["customer_email"]
        print(f"Sending email to {email_address}")
        
        # Record the email as sent
        sent_emails.add(email_address)
        
        # Print count of unique emails sent so far
        print(f"Total unique emails sent: {len(sent_emails)}")
