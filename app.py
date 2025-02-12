import pika
import os
import json
import time
from datetime import datetime

def send_message_to_queue(queue_name, message):
    """
    Send a JSON message to a RabbitMQ queue.

    Parameters:
        queue_name (str): Name of the RabbitMQ queue.
        message (dict): The message to send, in dictionary format.
    """
    # RabbitMQ connection parameters from environment variables
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))
    rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
    rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')

    if not all([rabbitmq_host, rabbitmq_user, rabbitmq_password]):
        raise ValueError("RabbitMQ connection details are not properly configured!")

    # Connection credentials
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        credentials=credentials
    ))
    
    channel = connection.channel()

    # Ensure the queue exists (optional if it's guaranteed to exist)
    channel.queue_declare(queue=queue_name, durable=True)

    # Convert message to JSON
    json_message = json.dumps(message)

    # Publish message
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json_message,
        properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
    )

    print(f"Message sent to queue {queue_name}: {json_message}")
    connection.close()

if __name__ == "__main__":
    queue_name = "json_queue"
    
    while True:
        # Create a new message with a dynamic timestamp
        message = {
            "event": "user_signup",
            "user_id": 12345,  # You can make this dynamic if needed
            "timestamp": (datetime.utcnow() + timedelta(hours=1)).isoformat() + "Z"
        }
        
        # Send the message to the queue
        send_message_to_queue(queue_name, message)
        
        # Wait for 30 seconds before sending the next message
        # time.sleep(30)
