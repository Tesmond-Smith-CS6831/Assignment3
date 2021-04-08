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
import socket
import zmq
import random
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

class Broker:

    def __init__(self):
        self.frontend_socket = None
        self.backend_socket = None
        self.context = None
        self.proxy = None
        self.tempPubPort = None
        self.zk_path = '/nodes/'
        self.zk_type1_leader_path = '/leader1/'
        self.zk_type2_leader_path = '/leader2/'
        self.replica_path = '/lb_replica/'
        self.leader1 = None
        self.leader2 = None
        self.connection_count = 0
        self.zookeeper = KazooClient(hosts='127.0.0.1:2181')
        self.zookeeper.start()

    def gen_nodes(self):
        # leader_path = "{}{}".format(self.zk_leader_path, "leadNode")
        # if self.zookeeper.exists(leader_path):
        #     self.leader = self.zookeeper.get(leader_path)
        # else:
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
            node_info = bytes("{},{}".format(port1, port2).encode("utf-8"))
            if self.zookeeper.exists(node_name):
                pass
            else:
                self.zookeeper.ensure_path(self.zk_path)
                self.zookeeper.create(node_name, node_info, None, ephemeral=True)
        self.set_leaders()

    def set_leaders(self):
        """Set our Loadbalanced Leaders"""
        gen_leader = self.zookeeper.Election(self.zk_path, "leader").contenders()
        if gen_leader:
            self.leader1 = gen_leader[-1]
            self.leader2 = gen_leader[-2]
        else:
            _nv = random.randint(0, 4)
            self.leader1 = self.zookeeper.get("{}node{}".format(self.zk_path, _nv))
            _nv2 = random.choice([i for i in range(0,4) if i not in [_nv]])
            self.leader2 = self.zookeeper.get("{}node{}".format(self.zk_path, _nv2))
        leader_type1_path = "{}{}".format(self.zk_type1_leader_path, "leadNode")
        leader_type2_path = "{}{}".format(self.zk_type2_leader_path, "leadNode")

        if not self.zookeeper.exists(leader_type1_path):
            self.zookeeper.ensure_path(self.zk_type1_leader_path)
            self.zookeeper.create(leader_type1_path, self.leader1[0], None, ephemeral=True)
        # else:
        #     self.zookeeper.delete(leader_type1_path)
        #     self.zookeeper.ensure_path(self.zk_type1_leader_path)
        #     self.zookeeper.create(leader_type1_path, self.leader1[0], None, ephemeral=True)
        print("New Type 1 Broker Node added to pool: {}".format(self.leader1))

        if not self.zookeeper.exists(leader_type2_path):
            self.zookeeper.ensure_path(self.zk_type2_leader_path)
            self.zookeeper.create(leader_type2_path, self.leader2[0], None, ephemeral=True)
        # else:
        #     self.zookeeper.delete(leader_type2_path)
        #     self.zookeeper.ensure_path(self.zk_type2_leader_path)
        #     self.zookeeper.create(leader_type2_path, self.leader2[0], None, ephemeral=True)
        print("New Type 2 Broker Node added to pool: {}".format(self.leader2))

    def create_loadbalance_replicas(self):
        """Create ephemeral (session bound) replicas of node"""
        print("Replicating leader...")
        replica_node_type1 = "{}{}".format(self.replica_path, "replicaNodeT1")
        replica_node_type2 = "{}{}".format(self.replica_path, "replicaNodeT2")

        if self.zookeeper.exists(replica_node_type1):
            self.zookeeper.delete(replica_node_type1)
        self.zookeeper.ensure_path(self.replica_path)
        self.zookeeper.create(replica_node_type1, self.leader1[0])

        if self.zookeeper.exists(replica_node_type2):
            self.zookeeper.delete(replica_node_type2)
        self.zookeeper.ensure_path(self.replica_path)
        self.zookeeper.create(replica_node_type2, self.leader2[0])

        print("... Replication Complete")

    def remove_temp_nodes(self):
        for i in range(5):
            node_name = "{}{}".format(self.zk_path, "node" + str(i))
            self.zookeeper.delete(node_name)

    def establish_broker(self):
        try:
            @self.zookeeper.DataWatch(self.zk_type1_leader_path+"leadNode")
            def watch_node(data, stat, event):
                if event and event.type == "DELETED":
                    leader_path = "{}{}".format(self.zk_type1_leader_path, "leadNode")
                    replica_node_type1 = "{}{}".format(self.replica_path, "replicaNodeT1")
                    print("Load balancer event detected")
                    self.leader1 = self.zookeeper.get(replica_node_type1)
                    self.zookeeper.ensure_path(self.zk_type1_leader_path)
                    try:
                        self.zookeeper.set(leader_path, self.leader1[0])
                    except NoNodeError:
                        self.zookeeper.create(leader_path, self.leader1[0])

            @self.zookeeper.DataWatch(self.zk_type2_leader_path+"leadNode")
            def watch_node(data, stat, event):
                if event and event.type == "DELETED":
                    leader_path = "{}{}".format(self.zk_type2_leader_path, "leadNode")
                    replica_node_type2 = "{}{}".format(self.replica_path, "replicaNodeT2")
                    print("Load balancer event detected")
                    self.leader2 = self.zookeeper.get(replica_node_type2)
                    self.zookeeper.ensure_path(self.zk_type2_leader_path)
                    try:
                        self.zookeeper.set(leader_path, self.leader2[0])
                    except NoNodeError:
                        self.zookeeper.create(leader_path, self.leader2[0])
        except NodeExistsError:
            pass
        print("Loadbalanced Broker established. RUNNING.")
        leader1_connection_addr = self.leader1[0].decode('utf-8').split(',')
        leader2_connection_addr = self.leader2[0].decode('utf-8').split(',')
        self.context = zmq.Context()
        self.frontend_socket = self.context.socket(zmq.XSUB)
        self.backend_socket = self.context.socket(zmq.XPUB)
        self.frontend_socket.bind(f"tcp://*:{leader1_connection_addr[0]}")
        self.backend_socket.bind(f"tcp://*:{leader1_connection_addr[1]}")

        self.frontend_socket.bind(f"tcp://*:{leader2_connection_addr[0]}")
        self.backend_socket.bind(f"tcp://*:{leader2_connection_addr[1]}")
        zmq.proxy(self.frontend_socket, self.backend_socket)


def publish_node_conn(publish_obj):
    up_status = False
    if publish_obj.pub_type == 1:
        if publish_obj.zookeeper.exists(publish_obj.zk_path_type1):
            up_status = True

        if up_status:
            data, stat = publish_obj.zookeeper.get(publish_obj.zk_path_type1)
            publish_obj.port = data.decode('utf-8').split(',')[0]
            conn_str = "tcp://" + publish_obj.host + ":" + publish_obj.port
            publish_obj.socket.connect(conn_str)
            print("ZK node connected")
        else:
            print("ZK not available")
    else:
        if publish_obj.zookeeper.exists(publish_obj.zk_path_type2):
            up_status = True

        if up_status:
            data, stat = publish_obj.zookeeper.get(publish_obj.zk_path_type2)
            publish_obj.port = data.decode('utf-8').split(',')[0]
            conn_str = "tcp://" + publish_obj.host + ":" + publish_obj.port
            publish_obj.socket.connect(conn_str)
            print("ZK node connected")
        else:
            print("ZK not available")


def subscribe_node_conn(subscriber_obj):
    up_status = False
    if subscriber_obj.zookeeper.exists(subscriber_obj.zk_path_type1) and subscriber_obj.zookeeper.exists(subscriber_obj.zk_path_type2):
        up_status = True

    if up_status:
        data1, stat = subscriber_obj.zookeeper.get(subscriber_obj.zk_path_type1)
        subscriber_obj.port1 = data1.decode('utf-8').split(',')[1]
        data2, stat = subscriber_obj.zookeeper.get(subscriber_obj.zk_path_type2)
        subscriber_obj.port2 = data2.decode('utf-8').split(',')[1]
        print("ZK node connected")
    else:
        print("ZK not available")


if __name__ == "__main__":
    broker = Broker()
    broker.gen_nodes()
    broker.create_loadbalance_replicas()
    broker.remove_temp_nodes()
    try:
        broker.establish_broker()
    except KeyboardInterrupt:
        broker.zookeeper.delete('/leader1/leadNode')
        broker.zookeeper.delete('/leader2/leadNode')
        print("Broker Node deleted")
