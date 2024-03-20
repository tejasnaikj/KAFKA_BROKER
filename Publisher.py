import socket
import ssl

broker_address = ("192.168.62.36", 5555)
client_socket = None

try:
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.load_verify_locations(cafile="ca-cert.pem")  
    context.check_hostname = False  
    client_socket = context.wrap_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    client_socket.connect(broker_address)
    print("Connected to broker.")

    while True:
        choice = input("Enter '1' to create a new topic, '2' to publish a message: ")

        if choice == '1':
            new_topic = input("Enter the name of the new topic: ")
            print(f"Creating new topic '{new_topic}'...")
            client_socket.send(f"CreateTopic:{new_topic}".encode('utf-8'))
            response = client_socket.recv(1024)
            print(response.decode())

        elif choice == '2':
            topic = input("Enter the name of the topic: ")
            message_content = input("Enter message to publish: ")
            full_message = f"Publish:{topic}:{message_content}"
            print(f"Publishing message to topic '{topic}'...")
            client_socket.send(full_message.encode('utf-8'))
            response = client_socket.recv(1024)
            print(response.decode())
        else:
            print("Invalid choice. Please enter '1' or '2'.")

except KeyboardInterrupt:
    print("Closing publisher...")

except ConnectionRefusedError:
    print("Connection to the broker refused. Make sure the broker is running and accessible.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if client_socket:
        client_socket.close()
