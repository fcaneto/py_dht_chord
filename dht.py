"""
This module implements a distributed hash table using the Chord protocol.
"""

import argparse
from threading import Thread
import itertools
import time

import proxy
from proxy import NodeClientProxy, KeyNotFoundException
from chord import ChordNode
from util import hash_function


class DHTNode(ChordNode):

    def __init__(self, ip, port, key=None):
        super(DHTNode, self).__init__(ip, port, key)
        self.hashtable = {}

    def get(self, key):
        hashed = hash_function(key)
        node_id = self.find_successor(hashed)
        
        if node_id == self.node_id:
            if key not in self.hashtable:
                raise KeyNotFoundException(key)

            return self.hashtable[key]
        else:
            return NodeClientProxy(node_id).get(key)

    def put(self, key, value):
        hashed = hash_function(key)
        node_id = self.find_successor(hashed)
        
        if node_id == self.node_id:
            self.hashtable[key] = value
        else:
            NodeClientProxy(node_id).put(key, value)

    def ping(self):
        return "pong"

    def show_table(self):
        for i in itertools.count():
            print("-- Node status #%s -------------------------" % i)
            for k,v in self.hashtable.items():
                print("%s => %s" % (k, v))
            print("--------------------------------------------")
            time.sleep(5)

    def run(self):
        super(DHTNode, self).run()
        Thread(target=self.show_table).start()
        proxy.start_server(self)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=int, help="Port this node is going to listen to.")
    parser.add_argument('--join', metavar="node_port", help="To which node this new node should join the network")
    parser.add_argument('--key', metavar="key", help="For testing purposes")

    args = parser.parse_args()

    print("Creating node on port %s" % args.port)

    if args.key:
        node = DHTNode(ip='localhost', port=args.port, key=int(args.key))    
    else:
        node = DHTNode(ip='localhost', port=args.port)
    
    print("%s CREATED" % node.node_id)

    if args.join:
        print("Joining node on %s" % args.join)
        node.join(ip='localhost', port=int(args.join))

    node.run()

