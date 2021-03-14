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
import threading
import random
from kazoo.client import KazooClient

ZK_BARRIER = False


class Broker(threading.Thread):

    def __init__(self, name, args):
        self.front = self.gen_ports()
        self.back = self.gen_ports()
        self.frontend_socket = None
        self.backend_socket = None
        self.context = None
        self.proxy = None
        self.tempPubPort = None

        threading.Thread.__init__(self, name=name)
        self.zkIPAddr = args['server']
        self.zkPort = args['port']
        self.cond = args['cond']
        self.ppath = args['ppath']
        self.zk = None
        # self.func = func
        self.spawned = False
        # self.barrier = False

    def gen_ports(self, exclude=None):
        return random.randint(1000, 9999)

    def establish_broker(self):
        try:
            if self.front == self.back:
                self.back = self.gen_ports()
            while True:
                self.context = zmq.Context()
                self.frontend_socket = self.context.socket(zmq.XSUB)
                self.backend_socket = self.context.socket(zmq.XPUB)
                self.frontend_socket.bind(f"tcp://*:{self.front}")
                self.backend_socket.bind(f"tcp://*:{self.back}")
                zmq.proxy(self.frontend_socket, self.backend_socket)

                if ZK_BARRIER == True:
                    # print(("AppThread {} barrier not reached yet".format(app.name)))
                    # if app.zk.exists(app.ppath):
                    #     value, stat = app.zk.get(app.ppath)
                    #     print(("AppThread {} found parent znode value = {}, stat = {}".format(app.name, value, stat)))
                    # else:
                    #     print(("{} znode does not exist yet (strange)".format(app.ppath)))

                    print(("AppThread {} has reached the barrier and so we disconnect from zookeeper".format(app.name)))
                    app.zk.stop()
                    app.zk.close()
                    print(("AppThread {}: Bye Bye ".format(app.name)))
                    break

        except zmq.error.ZMQError:
            self.front = self.gen_ports()
            self.back = self.gen_ports()
            self.establish_broker()

    def register_pub(self, publisher):
        publisher.context = zmq.Context()
        publisher.socket = publisher.context.socket(zmq.PUB)
        publisher.socket.connect(f"tcp://{self.zkIPAddr}:{self.front}")
        return publisher

    def pub_send(self, publisher, message, proxy = 2):
        zipcode, temperature, date_time = message.split(',')
        self.proxy = proxy
        if proxy == 1:
            publisher.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))
        else:
            self.tempPubPort = self.front + 1
            publisher.socket.connect(f"tcp://{publisher.host}:{self.tempPubPort}")
            publisher.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))

    def register_sub(self, subscriber):
        subscriber.context = zmq.Context()
        subscriber.socket = subscriber.context.socket(zmq.SUB)
        subscriber.socket.connect(f"tcp://{self.zkIPAddr}:{self.back}")
        subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        return subscriber

    def filter_message(self, subscriber):
        if self.proxy == 2:
            subscriber.socket.connect(f"tcp://{self.zkIPAddr}:{self.tempPubPort}")
            subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        else:
            subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        subscriber.message = subscriber.socket.recv_string()
        return subscriber

    def barrier_change(self):
        ZK_BARRIER = True

    def run(self):
        try:
            # print(("AppThread::run - Client {} now running and opening connection to zookeeper".format(self.name)))
            hosts = self.zkIPAddr + str(":") + str(self.zkPort)
            self.zk = KazooClient(hosts)
            # print(("App::run -- state = {}".format(self.zk.state)))
            self.zk.start()

            while True:
                if self.zk.exists(self.ppath):
                    # print("AppThread::run - parent znode is set")
                    self.zk.create(self.ppath + str("/") + self.name, value=bytes(self.name, 'utf-8'), ephemeral=True,
                                   makepath=True)
                    break
                else:
                    print("AppThread::run -- parent znode is not yet up")
            self.establish_broker()
            # self.func(self)

        except:
            print("Unexpected error in AppThread::run", sys.exc_info()[0])
            raise
#
#
#
# socket_to_pub = sys.argv[1] if len(sys.argv) > 1 else
# socket_to_sub = sys.argv[2] if len(sys.argv) > 2 else "5556"
# universal_broker = Broker(socket_to_pub, socket_to_sub)
#
# if __name__ == "__main__":
#     universal_broker.establish_broker()
