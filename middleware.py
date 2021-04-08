"""
Initial thoughts from Rick T:
    - creating anonymity is the goal
    - middleware could act as a hashtable with initialized buckets 'n' for number of zips
        - key: zipcode
        - value: weather stats
    - Publisher script calls into middleware hashtable to update value using ziptable[zip] = collected_weather
    - Subscriber script polls value from middleware using ziptable[zip] or ziptable.get(zip)

    - this allows for neither the client/server knowing the pub/sub servers
"""
import sys
import zmq
import random
from kazoo.client import KazooClient

class Broker:

    def __init__(self):
        self.frontend_socket = None
        self.backend_socket = None
        self.context = None
        self.proxy = None
        self.tempPubPort = None

        self.zk_path = '/nodes/'
        self.zk_leader_path = '/leader/'
        self.replica_path = '/lb_replica/'
        self.leader = None
        self.connection_count = 0
        self.zookeeper = KazooClient(hosts='127.0.0.1:2181')
        self.zookeeper.start()

    def gen_nodes(self):
        leader_path = "{}{}".format(self.zk_leader_path, "leadNode")
        if self.zookeeper.exists(leader_path):
            self.leader = self.zookeeper.get(leader_path)
        else:
            used_ports = []
            for i in range(5):
                port1 = str(random.randrange(5000, 9999, 2))
                if port1 in used_ports:
                    port1 = str(random.randrange(5000, 9999, 2))
                used_ports.append(port1)

                port2 = str(random.randrange(5000, 9999, 3))
                if port2 in used_ports:
                    port2 = str(random.randrange(5000, 9999, 2))
                used_ports.append(port2)

                node_name = "{}{}".format(self.zk_path, "node"+str(i))
                ports = bytes("{},{}".format(port1, port2).encode("utf-8"))
                if self.zookeeper.exists(node_name):
                    pass
                else:
                    self.zookeeper.ensure_path(self.zk_path)
                    self.zookeeper.create(node_name,ports)
            self.set_leader()

    def set_leader(self):
        gen_leader = self.zookeeper.Election(self.zk_path, "leader").contenders()
        if gen_leader:
            self.leader = gen_leader[-1]
        else:
            _nv = random.randint(0,4)
            self.leader = self.zookeeper.get("{}node{}".format(self.zk_path, _nv))
        print("New leader node: {}".format(self.leader))
        leader_path = "{}{}".format(self.zk_leader_path, "leadNode")

        if self.zookeeper.exists(leader_path):
            self.zookeeper.delete(leader_path)
        self.zookeeper.ensure_path(leader_path)
        self.zookeeper.set(leader_path, self.leader[0])

    def create_loadbalance_replicas(self):
        """Create ephemeral (session bound) replicas of node"""
        if not self.zookeeper.exists(self.replica_path):
            print("Replicating leader...")
            for i in range(0, 10):
                replica_node = "{}{}".format(self.replica_path, "replicaNode{}".format(i))
                self.zookeeper.ensure_path(self.replica_path)
                self.zookeeper.create(replica_node, self.leader[0], ephemeral=True)
            print("... Replication Complete")

    def establish_broker(self):
        @self.zookeeper.DataWatch(self.zk_leader_path)
        def watch_node(data, stat, event):
            if event:
                leader_path = "{}{}".format(self.zk_leader_path, "leadNode")
                print("Load balancer event detected: {}".format(event.type))
                _nv = random.randint(0, 9)
                self.leader = self.zookeeper.get("{}replicaNode{}".format(self.replica_path, _nv))
                self.zookeeper.ensure_path(leader_path)
                self.zookeeper.set(leader_path, self.leader[0])
                self.zookeeper.delete(self.replica_path)
                self.create_loadbalance_replicas()
        print("Loadbalanced Broker established. RUNNING.")
        leader_connection_addr = self.leader[0].decode('utf-8').split(',')
        self.context = zmq.Context()
        self.frontend_socket = self.context.socket(zmq.XSUB)
        self.backend_socket = self.context.socket(zmq.XPUB)
        self.frontend_socket.bind(f"tcp://*:{leader_connection_addr[0]}")
        self.backend_socket.bind(f"tcp://*:{leader_connection_addr[1]}")
        zmq.proxy(self.frontend_socket, self.backend_socket)


def publish_node_conn(publish_obj):
    up_status = False
    if publish_obj.zookeeper.exists(publish_obj.zk_path):
        up_status = True

    if up_status:
        data, stat = publish_obj.zookeeper.get(publish_obj.zk_path)
        publish_obj.port = data.decode('utf-8').split(',')[0]
        conn_str = "tcp://" + publish_obj.host + ":" + publish_obj.port
        publish_obj.socket.connect(conn_str)
        print("ZK node connected")
    else:
        print("ZK not available")


def subscribe_node_conn(subscriber_obj):
    up_status = False
    if subscriber_obj.zookeeper.exists(subscriber_obj.zk_path):
        up_status = True

    if up_status:
        data, stat = subscriber_obj.zookeeper.get(subscriber_obj.zk_path)
        subscriber_obj.port = data.decode('utf-8').split(',')[1]
        print("ZK node connected")
    else:
        print("ZK not available")


if __name__ == "__main__":
    broker = Broker()
    broker.gen_nodes()
    broker.create_loadbalance_replicas()
    broker.establish_broker()
