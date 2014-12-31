from threading import Thread
import thriftpy
from thriftpy.rpc import make_client, make_server

node_thrift = thriftpy.load("node.thrift", module_name="node_thrift")


class NodeClientProxy(object):
    def __init__(self, node_id):
        self.client = make_client(node_thrift.Node, node_id.ip, node_id.port)

    def get_predecessor(self):
        return self.client.get_predecessor()

    def get_successor(self):
        return self.client.get_successor()

    def find_successor(self, key):
        return self.client.find_successor(key)

    def get_closest_preceding_finger(self, key):
        return self.get_closest_preceding_finger(key)

    def notify(self, candidate_predecessor_id):
        self.client.notify(to_thrift_node_id(candidate_predecessor_id))
        
def to_thrift_node_id(node_id):
        if not node_id:
            return None

        struct = node_thrift.NodeId()
        struct.ip = node_id.ip
        struct.port = node_id.port
        struct.key = node_id.key
        return struct

class NodeServerProxy(object):
    def __init__(self, node):
        self.node = node

    def get_predecessor(self):
        return to_thrift_node_id(self.node.get_predecessor())

    def get_successor(self):
        return to_thrift_node_id(self.node.get_successor())

    def find_successor(self, key):
        print('call:FIND_SUCCESSOR of %s' % key)
        return to_thrift_node_id(self.node.find_successor(key))

    def get_closest_preceding_finger(self, key):
        return to_thrift_node_id(self.node.get_closest_preceding_finger(key))

    def notify(self, candidate_predecessor_id):
        self.node.notify(candidate_predecessor_id)


def start_server(node):
    server = make_server(node_thrift.Node, NodeServerProxy(node), node.node_id.ip, node.node_id.port)
    # Thread(target=server.serve)
    server.serve()