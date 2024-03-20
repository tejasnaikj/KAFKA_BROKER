import socket
import ssl
import threading
import time

class KafkaBroker:
    def __init__(self, host='0.0.0.0', port=5555, certfile='server-cert.pem', keyfile='server-key.pem'):
        self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        self.socket = self.context.wrap_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), server_side=True)
        self.socket.bind((host, port))
        self.subscriptions = {}  # Dictionary to store subscribers for each topic
        self.topic_messages = {}  # Dictionary to store messages for each topic
        self.real_time_consumers = {}  # Dictionary to store clients waiting for real-time consumption
        print("Kafka broker started.")

    def handle_requests(self):
        self.socket.listen(5)
        while True:
            client_socket, address = self.socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        while True:
            request = client_socket.recv(1024).decode('utf-8')
            if not request:
                break
            parts = request.split(":")

            if parts[0] == 'CreateTopic':
                print("Received request from publisher:", request)
                topic = parts[1]
                print("Topic:", topic)
                if topic not in self.subscriptions:
                    self.subscriptions[topic] = set()
                    self.topic_messages[topic] = []
                    client_socket.send(f"New topic created: {topic}".encode('utf-8'))
                else:
                    client_socket.send(f"Topic already exists!".encode('utf-8'))

            elif parts[0] == 'Subscribe':
                topic = parts[1]
                print("Received subscription request from client for topic:", topic)
                if topic in self.subscriptions:
                    self.subscriptions[topic].add(client_socket)
                    # Calculate offset as the length of the message list for the topic
                    offset = len(self.topic_messages[topic])
                    # Send the offset value back to the client
                    client_socket.send(f"Offset:{offset}".encode('utf-8'))
                else:
                    # Send message indicating the topic doesn't exist
                    client_socket.send(f"Topic '{topic}' does not exist!".encode('utf-8'))

            elif parts[0] == 'Publish':
                print("Received request from publisher:", request)
                topic = parts[1]
                message_content = ":".join(parts[2:])
                if topic not in self.subscriptions:
                    self.subscriptions[topic] = set()
                    self.topic_messages[topic] = []
                self.topic_messages[topic].append(message_content)
                # Print list of messages for the topic
                # print(f"Messages for topic '{topic}':")

                # for message in self.topic_messages[topic]:
                #     print(message)

                client_socket.send(f"Message published!".encode('utf-8'))

                # Print subscribers for the topic
                # print("Subscribers for topic:")
                # for subscriber in self.subscriptions.get(topic, []):
                #     print(subscriber)

                # Send the published message to real-time consumers
                if topic in self.real_time_consumers:
                    for consumer_socket in self.real_time_consumers[topic]:
                        consumer_socket.send(message_content.encode('utf-8'))

            elif parts[0] == 'ConsumeFromOffset':
                topic = parts[1]
                offset = int(parts[2])
                print(f"Received consume request for topic '{topic}' from offset {offset}")
                # Check if offset is valid
                if offset >= len(self.topic_messages[topic]):
                    client_socket.send("No new messages to consume.".encode('utf-8'))
                    new_offset = len(self.topic_messages[topic])
                    client_socket.send(f"{new_offset}".encode('utf-8'))
                    continue
                # Send messages from the offset value to the client
                for i in range(offset, len(self.topic_messages[topic])):
                    client_socket.send(self.topic_messages[topic][i].encode('utf-8'))
                    # Introduce a small delay to ensure messages are sent separately
                    time.sleep(0.1)
                client_socket.send("END".encode('utf-8'))  # Signal end of messages

                # Increment offset value in the client
                new_offset = len(self.topic_messages[topic])
                client_socket.send(f"{new_offset}".encode('utf-8'))

            elif parts[0] == 'RealTimeConsume':
                topic = parts[1]
                print(f"Real-time consumption initiated for topic '{topic}'")
                # Store client socket for real-time consumption
                if topic not in self.real_time_consumers:
                    self.real_time_consumers[topic] = set()
                self.real_time_consumers[topic].add(client_socket)

            # Print subscribers for each topic
            print("Subscribers for each topic:")
            for topic, subscribers in self.subscriptions.items():
                subscriber_addresses = [subscriber.getpeername() for subscriber in subscribers]
                print(f"Topic: {topic}, Subscribers: {', '.join(str(addr) for addr in subscriber_addresses)}")

    def start(self):
        self.handle_requests()

    def stop(self):
        self.socket.close()
        print("Kafka broker stopped.")

# Create and start Kafka broker
broker = KafkaBroker()
broker.start()
