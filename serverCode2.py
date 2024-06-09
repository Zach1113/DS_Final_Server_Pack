import time
import socket
from kazoo.client import KazooClient # type: ignore
from kazoo.recipe.lock import Lock # type: ignore

def generate_log_entry(lv, msg):
    from datetime import datetime
    log_levels = ["INFO", "WARNING", "ERROR"]

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = log_levels[lv % len(log_levels)]
    return f"{timestamp} - {level} - {msg}"


def store_log_entry(zk, log_path, log_entry):
    zk.create(log_path + "/entry_", log_entry.encode('utf-8'), sequence=True)
    #print(f"Log entry {entry_number} stored in {znode_path}")

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
port_number = 8002
my_IP = socket.gethostbyname(socket.gethostname())
#my_IP = "192.168.66.32"
myPath = zk.create("/server/server_", (my_IP + ":" + str(port_number)).encode('utf-8'), ephemeral=True, sequence=True)
logPath = zk.create("/data/log/server_", sequence=True, makepath=True)

# Start provide service
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    server_socket.bind((my_IP, port_number))
    server_socket.listen()
    msg = f"Server start providing service on {my_IP}:{port_number}..."
    print(msg)
    logEntry = generate_log_entry(0, msg)
    store_log_entry(zk, log_path=logPath, log_entry=logEntry)

    while True:
        client_socket, client_address = server_socket.accept()
        
        msg = f"Receive connection from: {client_address}"
        print(msg)
        logEntry = generate_log_entry(0, msg)
        store_log_entry(zk, log_path=logPath, log_entry=logEntry)

        target_ticket = client_socket.recv(1024).decode()
        ticket_path = "/data/ticket/" + target_ticket 
        ticket_lock = Lock(zk, ticket_path + "/lock")

        # try to acquire ticket lock
        while True:
            print("Waiting for lock...")
            if ticket_lock.acquire(blocking=False): 
                msg = "Ticket lock for " + target_ticket + " acquired"
                print(msg)
                logEntry = generate_log_entry(0, msg)
                store_log_entry(zk, log_path=logPath, log_entry=logEntry)
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
                logEntry = generate_log_entry(0, replyMsg)
                store_log_entry(zk, log_path=logPath, log_entry=logEntry)
                print(replyMsg)
                client_socket.sendall(replyMsg.encode())
            else:
                replyMsg = f"{target_ticket} ticket not available"
                logEntry = generate_log_entry(1, replyMsg)
                store_log_entry(zk, log_path=logPath, log_entry=logEntry) 
                print(replyMsg)
                client_socket.sendall(replyMsg.encode())

        except Exception as e:
            replyMsg = f"Error during operating lock for ticket type: {target_ticket}"
            logEntry = generate_log_entry(2, replyMsg)
            store_log_entry(zk, log_path=logPath, log_entry=logEntry)            
            print(replyMsg)
            client_socket.sendall(replyMsg.encode())

        finally:
            # release ticket lock
            ticket_lock.release()
            msg = "lock released"
            logEntry = generate_log_entry(0, msg)
            store_log_entry(zk, log_path=logPath, log_entry=logEntry)
            print(msg)
    
except Exception as e:
    msg = f"Server error: {e}"
    logEntry = generate_log_entry(0, msg)
    store_log_entry(zk, log_path=logPath, log_entry=logEntry)
    print(msg)

zk.stop()