"""
A very simple command line interface for the DHT.
"""
import argparse

from proxy import NodeClientProxy, KeyNotFoundException
from chord import NodeId

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=int, help="The port it should connect to.")

    args = parser.parse_args()

    print("Connecting to localhost:%s..." % args.port)
    node_proxy = NodeClientProxy(NodeId(ip='localhost', port=args.port))
    print("OK!")
        

    print("Commands are:")
    print("- get <key>: saves a value in dht")
    print("- put <key> <value>: retrieves a value from dht")
    # print("- snapshot: prints current dht state")
    while True:
        command = input().split()
        if command[0] == "get":
            key = command[1]

            try:
                value = node_proxy.get(key)
                print("[%s] = %s" % (key, value))
            except KeyNotFoundException:
                print("Key not found.")

        elif command[0] == "put":
            key = command[1]
            value = command[2]
            node_proxy.put(key, value)
            print("Done.")

        else:
            print("Can't do that.")

        