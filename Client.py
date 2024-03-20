import ssl
import socket

broker_address = ("192.168.62.36", 5555)
offsets = {}  

try:
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.load_verify_locations("ca-cert.pem")  
    context.check_hostname = False 
    client_socket = context.wrap_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    client_socket.connect(broker_address)
    print("Connected to broker.")

    while True:
        choice = input("Enter '1' to subscribe to a new topic, '2' to consume messages, '3' for real-time consumption: ")

        if choice == '1':
            # Subscribe to a topic
            topic = input("Enter the name of the topic to subscribe: ")
            print(f"Subscribing to topic '{topic}'...")
            client_socket.send(f"Subscribe:{topic}".encode('utf-8'))
            
            response = client_socket.recv(1024).decode('utf-8')
            if response.startswith("Topic"):
                print(response)
            else:
                offset_value = int(response.split(":")[1])
                offsets[topic] = offset_value
                print("Subscription added!")
                print(f"Offset for topic '{topic}': {offsets[topic]}")

        elif choice == '2':
            topic = input("Enter the name of the topic: ")
            if topic not in offsets:
                print("You need to subscribe to this topic first.")
                continue

            offset_choice = input(f"Enter '1' to use default offset ({offsets[topic]}), 0 to enter a custom offset: ")
            if offset_choice == '1':
                offset_to_use = offsets[topic]
            else:
                offset_to_use = int(input("Enter the custom offset value: "))

            print(f"Consuming messages from offset {offset_to_use} for topic '{topic}'...")
            client_socket.send(f"ConsumeFromOffset:{topic}:{offset_to_use}".encode('utf-8'))

            while True:
                message = client_socket.recv(1024).decode('utf-8')
                if message == "END":
                    break
                if message.startswith("No"):
                    print(message)
                    break
                print(message)

            new_offset = int(client_socket.recv(1024).decode('utf-8'))
            offsets[topic] = new_offset
            print(f"New offset for topic '{topic}': {offsets[topic]}")

        elif choice == '3':
            topic = input("Enter the name of the topic to consume real-time messages: ")
            print(f"Starting real-time consumption of messages for topic '{topic}'...")
            client_socket.send(f"RealTimeConsume:{topic}".encode('utf-8'))

            while True:
                message = client_socket.recv(1024).decode('utf-8')
                if message == "END":
                    break
                if message.startswith("No"):
                    print(message)
                    break
                print(message)

        else:
            print("Invalid choice. Please enter '1', '2', or '3'.")

except KeyboardInterrupt:
    print("Closing client...")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    client_socket.close()