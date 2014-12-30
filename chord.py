import hashlib
import time
from threading import Thread, Lock

from util import Interval, RING_SIZE, M

sim_lock = Lock()

"""
- How are collision in the identifier circle handled?
"""

def my_hash(key):
    #h = hashlib.sha1()
    #h.update(str(key).encode())
    #return int(h.hexdigest(), 16)
    #return int(h.hexdigest(), 16) % RING_SIZE
    return int(key[:-1])

class NodeId(object):

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.key = my_hash("%s:%s" % (ip, port))

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


class ChordNode(Thread):
    def __init__(self, ip, port):
        self.node_id = NodeId(ip, port)
        self.key = self.node_id.key

        self.predecessor_id = None

        # Initializing finger table interval starts.
        self.fingertable = []
        for i in range(1, M + 1):
            finger_start = (self.key + 2 ** (i - 1)) % RING_SIZE
            self.fingertable.append(FingerTableEntry(start=finger_start, node_id=self.node_id))

        Thread.__init__(self)

    def __str__(self):
        s = "Node %s - Predecessor: %s \n  Fingertable:" % (self.node_id, self.predecessor_id)
        for entry in self.fingertable:
            s += "\n\t%s" % entry
        return s

    def find_successor(self, key):
        predecessor_id = self.find_predecessor(key)
        return rpc_call(predecessor_id, 'get_successor')

    def find_predecessor(self, key):
        current_node_id = self.node_id
        current_successor_id = self.get_successor()

        if current_node_id != current_successor_id:  # if there are more than one node in the network
            while key not in Interval(current_node_id.key, current_successor_id.key,
                                      closed_on_left=False, closed_on_right=True):
                print("Finding predecessor of [%s]: it's not between %s and %s"
                      % (key, current_node_id.key, current_successor_id.key))
                current_node_id = rpc_call(current_node_id, 'get_closest_preceding_finger', key)
                current_successor_id = rpc_call(current_node_id, 'get_successor')
        return current_node_id

    def get_closest_preceding_finger(self, key):
        for entry in reversed(self.fingertable):
            if key in Interval(entry.node_id.key, self.key, closed_on_left=False, closed_on_right=False):
                return entry.node_id
        return self.node_id

    def update_fingertable(self, new_node_id_index):
        new_node_id, index = new_node_id_index

        # is new_node in (self, finger[i].node?
        assertion = new_node_id.key in Interval(self.node_id.key, self.fingertable[index].node_id.key,
                                                closed_on_left=False, closed_on_right=False)
        print("update ft: is %s in [%s, %s)? %s" % (new_node_id.key, self.node_id.key,
                                                 self.fingertable[index].node_id.key, assertion))

        if assertion:
            print("update ft: YES, update ft[%s] with %s" % (index, new_node_id.key))
            self.fingertable[index].node_id = new_node_id
            predecessor = self.predecessor_id
            print("update ft: RECURSIVE CALL on %s" % predecessor.key)
            rpc_call(predecessor, "update_fingertable", arg=new_node_id_index)

        #predecessor =

    def get_predecessor(self):
        return self.predecessor_id

    def set_predecessor(self, node):
        self.predecessor_id = node

    def get_successor(self):
        return self.fingertable[0].node_id

    def join(self, other_node_id=None):
       print('JOIN')
       r = rpc_call(other_node_id, "find_successor", self.node_id.key)
       self.fingertable[0].node_id = r

    # def join(self, online_node_id=None):
    #     """
    #     """
    #     if online_node_id:
    #         # find new_node successor:
    #         print("JOIN STARTS: INIT FINGERTABLE:")
    #         self.init_fingertable(online_node_id)
    #         print("------------------------------")
    #         print("JOIN: UPDATING OTHERS")
    #         self.update_others()
    #     else:
    #         # first node in network
    #         for entry in self.fingertable:
    #             entry.node_id = self.node_id
    #         self.predecessor_id = self.node_id

    # def init_fingertable(self, online_node_id):
    #     successor = rpc_call(online_node_id, 'find_successor', self.fingertable[0].start)
    #     self.fingertable[0].node_id = successor
    #     self.predecessor_id = rpc_call(successor, 'get_predecessor')
    #     rpc_call(successor, 'set_predecessor', self.node_id)

    #     for i in range(0, len(self.fingertable) - 1):
    #         print("Setting finger[%s] (start=%s)" % (i + 1, self.fingertable[i + 1].start))
    #         if self.fingertable[i + 1].start in Interval(self.key, self.fingertable[i].node_id.key):
    #             print("%s is between %s and %s" % (self.fingertable[i + 1].start, self.key,
    #                                                self.fingertable[i].node_id.key))
    #             self.fingertable[i + 1].node_id = self.fingertable[i].node_id
    #         else:
    #             self.fingertable[i + 1].node_id = rpc_call(online_node_id, 'find_successor', self.fingertable[i + 1].start)

    # def update_others(self):
    #     for i in range(0, M):
    #         node_id = self.find_predecessor((self.key - 2 ** i) % RING_SIZE)
    #         print("Updating others: who is the predecessor of %s? => %s" % ((self.key - 2 ** i) % RING_SIZE, node_id))
    #         print("Update fingertable[%s] on node %s with %s" % (i, node_id.key, self.node_id.key))
    #         rpc_call(node_id, "update_fingertable", (self.node_id, i))


    def stabilize(self):
        sim_lock.acquire()

        x = rpc_call(self.get_successor(), "get_predecessor")

        import pdb
        pdb.set_trace()

        print("%s : stabilize" % self.node_id)
        print("is %s in (%s, %s]?" % (x.key, self.node_id.key, self.get_successor.key))

        if x and x.key in Interval(self.node_id.key, self.get_successor.key,
                                                closed_on_left=False, closed_on_right=True):
            pass

        rpc_call(self.get_successor(), "notify", self.node_id)

        sim_lock.release()


    def notify(self, predecessor_candidate_id):
        if self.predecessor is None:
            self.predecessor_id = predecessor_candidate_id
        else:
            between_predecessor_and_me = Interval(self.node_id.key, self.get_successor.key,
                                                closed_on_left=True, closed_on_right=False)
            if predecessor_candidate_id.key in between_predecessor_and_me:
                self.predecessor_id = predecessor_candidate_id

    def run(self):
        while True:
            self.stabilize()
            time.sleep(2)
            print("Node %s woke up" % self.node_id)


def rpc_call(target_node_id, name, arg=None):
    print("Calling %s on %s with arg = %s" % (name, target_node_id, arg))

    return_value = None

    key = "%s:%s" % (target_node_id.ip, target_node_id.port)
    if key in Simulation.nodes:
        if arg is not None:
            return_value = getattr(Simulation.nodes[key], name)(arg)
        else:
            return_value = getattr(Simulation.nodes[key], name)()
    else:
        print("ERROR: Node (%s,%s) does not exist." % target_node_id.ip, target_node_id.port)

    return return_value




class Simulation():

    nodes = {}

    def __init__(self):
        self.first_node = None

    def create_node(self, ip, port):
        node = ChordNode(ip, port)
        node.start()

        if not self.first_node:
            self.first_node = node
        self.nodes["%s:%s" % (ip, port)] = node

        if len(self.nodes.values()) == 1:
            print("Joining as first:")
            # node.join()
        else:
            any_node_id = self.first_node.node_id
            print("Joining to %s:" % any_node_id)
            node.join(any_node_id)

    def print_status(self):
        sim_lock.acquire()
        print("-- Simulation status -----------------------")
        nodes = sorted(self.nodes.values(), key=lambda n: n.key)
        for node in nodes:
           print(node)
        print("--------------------------------------------")
        sim_lock.release()

    def loop(self):
        command = ""

        #print("Creating node")
        #self.create_node("localhost", "9001")
        self.create_node("3", "")

        #print("Creating node")
        #self.create_node("localhost", "9002")
        self.create_node("0", "")

        while True:
           time.sleep(10)
           self.print_status()

        #print("Creating node")
        #self.create_node("localhost", "9003")
        #self.create_node("1", "")

        


if __name__ == "__main__":
    Simulation().loop()
