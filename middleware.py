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
        # self.front = frontend_port
        # self.back = backend_port
        self.frontend_socket = None
        self.backend_socket = None
        self.context = None
        self.proxy = None
        self.tempPubPort = None

        self.zk_path = '/nodes/'
        self.zk_leader_path = '/leader/'
        self.leader = None
        self.zookeeper = KazooClient(hosts='127.0.0.1:2181')
        self.zookeeper.start()

    def gen_nodes(self):
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
        self.zookeeper.set(leader_path,self.leader[0])


    def establish_broker(self):
        leader_connection_addr = self.leader[0].decode('utf-8').split(',')
        self.context = zmq.Context()
        self.frontend_socket = self.context.socket(zmq.XSUB)
        self.backend_socket = self.context.socket(zmq.XPUB)
        self.frontend_socket.bind(f"tcp://*:{leader_connection_addr[0]}")
        self.backend_socket.bind(f"tcp://*:{leader_connection_addr[1]}")
        zmq.proxy(self.frontend_socket, self.backend_socket)

    # def register_pub(self, publisher):
    #     publisher.context = zmq.Context()
    #     publisher.socket = publisher.context.socket(zmq.PUB)
    #     publisher.socket.connect(f"tcp://{publisher.host}:{publisher.port}")
    #     return publisher
    #
    # def pub_send(self, publisher, message, proxy = 2):
    #     zipcode, temperature, date_time = message.split(',')
    #     self.proxy = proxy
    #     if proxy == 1:
    #         publisher.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))
    #     else:
    #         self.tempPubPort = int(publisher.port) + 1
    #         publisher.socket.connect(f"tcp://{publisher.host}:{self.tempPubPort}")
    #         publisher.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))

    def register_sub(self, subscriber):
        subscriber.context = zmq.Context()
        subscriber.socket = subscriber.context.socket(zmq.SUB)
        subscriber.socket.connect(f"tcp://{subscriber.address}:{subscriber.port}")
        subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        return subscriber

    def filter_message(self, subscriber):
        if self.proxy == 2:
            subscriber.socket.connect(f"tcp://{subscriber.address}:{self.tempPubPort}")
            subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        else:
            subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
            subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        subscriber.message = subscriber.socket.recv_string()
        return subscriber


if __name__ == "__main__":
    broker = Broker()
    broker.gen_nodes()
    broker.set_leader()
    broker.establish_broker()
    # universal_broker.establish_broker()
