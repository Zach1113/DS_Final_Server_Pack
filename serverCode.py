import time
import socket
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock

host = '127.0.1.1:2181'
zk = KazooClient(hosts=host)
zk.start()

# first server should set up znode tree
if not zk.exists("/server"):
    zk.create("/server", makepath=True)
    default_quantity = 3
    allTicketType = ["A", "B", "C"]
    for ticketType in allTicketType:
        zk.create(f"/data/ticket/{ticketType}/quantity", str(default_quantity).encode("utf-8"), makepath=True)

# create reference node of this server on zookeeper
port_number = 8000
my_IP = socket.gethostbyname(socket.gethostname())
myPath = zk.create("/server/server_", (my_IP + ":" + str(port_number)).encode('utf-8'), ephemeral=True, sequence=True)

# Start provide service
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    server_socket.bind((my_IP, port_number))
    server_socket.listen()
    print(f"Server start providing service on {my_IP}:{port_number}...")

    while True:
        client_socket, client_address = server_socket.accept()
        
        print(f"Receive connection from client: {client_address}")

        target_ticket = client_socket.recv(1024).decode()
        ticket_path = "/data/ticket/" + target_ticket 
        ticket_lock = Lock(zk, ticket_path + "/lock")

        # try to acquire ticket lock
        while True:
            print("Waiting for lock...")
            if ticket_lock.acquire(blocking=False): 
                print("-> ticket lock for " + target_ticket + " acquired")
                break
            time.sleep(1)

        try: 
            # check the quantity of target ticket and modify ticket quantity
            data, stat = zk.get(ticket_path + "/quantity")
            ticket_quantity = int(data.decode("utf-8"))
            if ticket_quantity > 0:
                ticket_quantity -= 1
                new_data = str(ticket_quantity).encode('utf-8')
                zk.set(ticket_path + "/quantity", new_data)    
                replyMsg = f"{target_ticket} ticket purchased!"
                print(replyMsg)
                client_socket.sendall(replyMsg.encode())
            else:
                replyMsg = f"{target_ticket} ticket not available" 
                print(replyMsg)
                client_socket.sendall(replyMsg.encode())

        except Exception as e:
            replyMsg = f"Error during operating lock for ticket type: {target_ticket}"
            print(replyMsg)
            client_socket.sendall(replyMsg.encode())

        finally:
            # release ticket lock
            ticket_lock.release()
            print("lock released")
    
except Exception as e:
    print(f"Server error: {e}")

zk.stop()