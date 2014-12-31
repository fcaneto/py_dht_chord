import hashlib
import time
import argparse
from threading import Thread, Lock
import itertools

import proxy
from proxy import NodeClientProxy, NodeServerProxy
from util import Interval, RING_SIZE, M

sim_lock = Lock()

"""
- How are collision in the identifier circle handled?
"""

def hash_function(key):
    h = hashlib.sha1()
    h.update(str(key).encode())
    return int(h.hexdigest(), 16)

class NodeId(object):

    def __init__(self, ip, port, key=None):
        self.ip = ip
        self.port = port

        if key is not None:
            self.key = key
        else:
            self.key = hash_function("%s:%s" % (ip, port))

    def __str__(self):
        #return "[%s] %s:%s" % (self.key, self.ip, self.port)
        return "[%s]" % self.key

class FingerTableEntry:

    def __init__(self, start, node_id=None):
        self.start = start
        self.node_id = node_id

    def __str__(self):
        if self.node_id:
            return "%s => %s" % (self.start, self.node_id)
        else:
            return "%s => no node" % (self.start)


class ChordNode():
    def __init__(self, ip, port, key=None):
        self.node_id = NodeId(ip, port, key)
        self.key = self.node_id.key

        self.predecessor_id = self.node_id

        # Initializing finger table interval starts.
        self.fingertable = []
        for i in range(1, M + 1):
            finger_start = (self.key + 2 ** (i - 1)) % RING_SIZE
            self.fingertable.append(FingerTableEntry(start=finger_start, node_id=self.node_id))

    def __str__(self):
        s = "Node %s - Predecessor: %s \n  Fingertable:" % (self.node_id, self.predecessor_id)
        for entry in self.fingertable:
            s += "\n\t%s" % entry
        return s

    def find_successor(self, key):
        predecessor_id = self.find_predecessor(key)
        if predecessor_id != self.node_id:
            s = NodeClientProxy(predecessor_id).get_successor()
        else:
            s = self.get_successor()
        return s

    def find_predecessor(self, key):
        current_node_id = self.node_id
        current_successor_id = self.get_successor()

        # if current_node_id != current_successor_id:  # if there are more than one node in the network
        while key not in Interval(current_node_id.key, current_successor_id.key,
                                  closed_on_left=False, closed_on_right=True):
            print("Finding predecessor of [%s]: it's not between %s and %s"
                  % (key, current_node_id.key, current_successor_id.key))

            if current_node_id == self.node_id:
                print("Asking self the CPF")
                current_node_id = self.get_closest_preceding_finger(key)
            else:
                print("Asking %s the CPF" % current_node_id)
                current_node_id = NodeClientProxy(current_node_id).get_closest_preceding_finger(key)
            
            current_successor_id = NodeClientProxy(current_node_id).get_successor()
        return current_node_id

    def get_closest_preceding_finger(self, key):
        for i, entry in enumerate(reversed(self.fingertable)):
            print("CPF: entry = %s" % entry.node_id)
            if entry.node_id.key in Interval(self.key, key, closed_on_left=False, closed_on_right=False):
                print("CPF returning %s" % entry.node_id)
                return entry.node_id
        print("CPF returning self")
        return self.node_id

    def get_predecessor(self):
        return self.predecessor_id

    def set_predecessor(self, node):
        self.predecessor_id = node

    def get_successor(self):
        return self.fingertable[0].node_id

    def set_successor(self, node_id):
        self.fingertable[0].node_id = node_id

    def join(self, ip, port):
       print('%s JOINING...' % self.node_id)
       r = NodeClientProxy(NodeId(ip, port)).find_successor(self.node_id.key)
       print('%s successor is %s' % (self.node_id, r))
       self.fingertable[0].node_id = r


    def stabilize(self):
        sim_lock.acquire()

        successor_id = self.get_successor()
        if successor_id != self.node_id:
            x = NodeClientProxy(successor_id).get_predecessor()
        else:
            x = self.get_predecessor()

            # import pdb
            # pdb.set_trace()

        # print("%s : stabilize" % self.node_id)
        # print("is %s in (%s, %s]?" % (x, self.node_id.key, self.get_successor().key))

        if x and x.key in Interval(self.node_id.key, self.get_successor().key,
                                                closed_on_left=False, closed_on_right=True):
            # print('Yes!')
            self.set_successor(x)

        if successor_id != self.node_id:
            NodeClientProxy(self.get_successor()).notify(self.node_id)

        sim_lock.release()

    def fix_finger(self, index):
        print("Fixing finger %s:%s" % (index, self.fingertable[index].start))
        new_finger_node_id = self.find_successor(self.fingertable[index].start)
        self.fingertable[index].node_id = new_finger_node_id

    def notify(self, predecessor_candidate_id):
        # print("NOTIFY: hmmm, %s thinks it might be my predecessor..." % predecessor_candidate_id)

        if self.predecessor_id is None:
            self.predecessor_id = predecessor_candidate_id
        else:
            between_predecessor_and_me = Interval(self.get_predecessor().key, self.key,
                                                closed_on_left=True, closed_on_right=False)
            if predecessor_candidate_id.key in between_predecessor_and_me:
                self.predecessor_id = predecessor_candidate_id
                print("NOTIFY: ... and %s is!" % predecessor_candidate_id)

    def show_status(self):
        for i in itertools.count():
            print("-- Node status #%s -------------------------" % i)
            print(self)
            print("--------------------------------------------")
            time.sleep(5)

    def monitoring(self):
        i = 0
        while True:
            self.stabilize()
            self.fix_finger(i)
            i = 0 if i == M-1 else i + 1
            time.sleep(1)

    def run(self):
        Thread(target=self.monitoring).start()
        Thread(target=self.show_status).start()
        proxy.start_server(self)
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=int, help="Port node is going to listen to.")
    parser.add_argument('--join', metavar="node_port", help="Make the new node join the network")
    parser.add_argument('--key', metavar="key", help="Easter egg")

    args = parser.parse_args()

    print("Creating node on port %s" % args.port)

    if args.key:
        node = ChordNode(ip='localhost', port=args.port, key=int(args.key))    
    else:
        node = ChordNode(ip='localhost', port=args.port)
    
    print("%s CREATED" % node.node_id)

    if args.join:
        print("Joining node on %s" % args.join)
        node.join(ip='localhost', port=int(args.join))

    node.run()



