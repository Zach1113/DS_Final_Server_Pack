from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, BadArgumentsError

zk = KazooClient(hosts='127.0.1.1:2181')
zk.start()

def delete_znode_recursive(zk, path):
    try:
        children = zk.get_children(path)
        for child in children:
            child_path = f"{path}/{child}"
            delete_znode_recursive(zk, child_path)
        if path != "/":
            zk.delete(path)
            print(f"Deleted {path}")
    except NoNodeError:
        print(f"Node {path} does not exist")
    except BadArgumentsError as e:
        print(f"BadArgumentsError for node {path}: {e}")

def resetTicketQuantity(zk, quantity):
    ticketPath = "/data/ticket"
    children = zk.get_children(ticketPath)
    try:
        for child in children:
            zk.set(f"{ticketPath}/{child}/quantity", str(quantity).encode("utf-8"))
    except Exception as e:
        print(f"Failed to set quantity for {child}: {e}")

while True:
    print("Enter 1 for ticket quantity reset")
    print("Enter 2 for znode tree deletion")
    choice = input()

    if choice == "1":
        quantity = int(input("Enter quantity: "))
        resetTicketQuantity(zk, quantity)
        print("Ticket quantity reset successed!")
        break

    elif choice == "2":
        print("Please ensure you've closed all the servers")
        input("Press enter to continue...")
        root_path = "/"
        delete_znode_recursive(zk, root_path)
        print("Znode tree deletion completed!")
        break

    else:
        print("Invalid choice, please try again...")

zk.stop()