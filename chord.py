"""
This module implements a Chord overlay network, with stabilization algorithm for 
dealing with concurrent node joins.

Things I didn't care:
- Key collisions: both in the node id space and dht key space

"""

import time
from threading import Thread, Lock
import itertools

import proxy
from proxy import NodeClientProxy
from util import hash_function, Interval, RING_SIZE, M, NodeId


class FingerTableEntry(object):

    def __init__(self, start, node_id=None):
        self.start = start
        self.node_id = node_id

    def __str__(self):
        if self.node_id:
            return "%s => %s" % (self.start, self.node_id)
        else:
            return "%s => no node" % (self.start)


class ChordNode(object):
    def __init__(self, ip, port, key=None):
        self.sim_lock = Lock() # lock for concurrent beautiful prints

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
        """
        Given a key, it finds the node responsible for it in the network.
        """
        predecessor_id = self.find_predecessor(key)
        if predecessor_id != self.node_id:
            s = NodeClientProxy(predecessor_id).get_successor()
        else:
            s = self.get_successor()
        return s

    def find_predecessor(self, key):
        """
        Given a key, it finds the immediate node preceding it in the ring.
        """
        current_node_id = self.node_id
        current_successor_id = self.get_successor()

        while key not in Interval(current_node_id.key, current_successor_id.key,
                                  closed_on_left=False, closed_on_right=True):
            # print("Finding predecessor of [%s]: it's not between %s and %s"
                  # % (key, current_node_id.key, current_successor_id.key))

            if current_node_id == self.node_id:
                # print("Asking self the CPF")
                current_node_id = self.get_closest_preceding_finger(key)
            else:
                # print("Asking %s the CPF" % current_node_id)
                current_node_id = NodeClientProxy(current_node_id).get_closest_preceding_finger(key)
            
            current_successor_id = NodeClientProxy(current_node_id).get_successor()
        return current_node_id

    def get_closest_preceding_finger(self, key):
        for i, entry in enumerate(reversed(self.fingertable)):
            # print("CPF: entry = %s" % entry.node_id)
            if entry.node_id.key in Interval(self.key, key, closed_on_left=False, closed_on_right=False):
                # print("CPF returning %s" % entry.node_id)
                return entry.node_id
        # print("CPF returning self")
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
       # print('%s successor is %s' % (self.node_id, r))
       self.fingertable[0].node_id = r

    def stabilize(self):
        self.sim_lock.acquire()

        successor_id = self.get_successor()
        if successor_id != self.node_id:
            x = NodeClientProxy(successor_id).get_predecessor()
        else:
            x = self.get_predecessor()

        if x and x.key in Interval(self.node_id.key, self.get_successor().key,
                                                closed_on_left=False, closed_on_right=True):
            self.set_successor(x)

        if successor_id != self.node_id:
            NodeClientProxy(self.get_successor()).notify(self.node_id)

        self.sim_lock.release()

    def fix_finger(self, index):
        # print("Fixing finger %s:%s" % (index, self.fingertable[index].start))
        new_finger_node_id = self.find_successor(self.fingertable[index].start)
        self.fingertable[index].node_id = new_finger_node_id

    def notify(self, predecessor_candidate_id):
        if self.predecessor_id is None:
            self.predecessor_id = predecessor_candidate_id
        else:
            between_predecessor_and_me = Interval(self.get_predecessor().key, self.key,
                                                closed_on_left=True, closed_on_right=False)
            if predecessor_candidate_id.key in between_predecessor_and_me:
                self.predecessor_id = predecessor_candidate_id
                # print("NOTIFY: ... and %s is!" % predecessor_candidate_id)

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
        # Thread(target=self.show_status).start()
        
        




