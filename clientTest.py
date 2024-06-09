import socket
import time

class MainClient:

    @staticmethod
    def connect_to_server(ip, port):
        while True:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                client_socket.connect((ip, port))

                ticketType = "A"
                client_socket.sendall(ticketType.encode())

                message = client_socket.recv(1024).decode()
                print(f"Received message: {message}")

                time.sleep(1)

                client_socket.close()
                break

            except Exception as e:
                print(f"connect failed!: {e}")
                client_socket.close()
                break

MainClient.connect_to_server("127.0.1.1", 8003)