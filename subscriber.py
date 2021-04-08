import sys
import datetime
import middleware
from utility_funcs import register_sub, filter_message, receive_history
from kazoo.client import KazooClient


class Subscriber:
    def __init__(self, address, topic, time_to_listen, history_size, history = 2223):
        self.address = address
        self.port1 = None
        self.port2 = None
        self.total_temp = 0
        self.zip_code = topic
        self.total_times_to_listen = int(time_to_listen)
        self.listen_counter = 0
        self.history_socket = None
        self.history_port = history
        self.history_size = int(history_size)
        self.history_array = []
        self.message = None
        self.context = None
        self.socket = None
        self.output = []
        self.zookeeper = KazooClient(hosts='127.0.0.1:2181')
        self.zk_path_type1 = '/leader1/leadNode'
        self.zk_path_type2 = '/leader2/leadNode'
        self.zookeeper.start()

    def create_context(self):
        self = register_sub(self)

    def middleware_port_connection(self):
        self = middleware.subscribe_node_conn(self)

    def get_message(self):
        print("Pulling data from: tcp://{} on ports {} and {}".format(self.address, self.port1, self.port2))

        @self.zookeeper.DataWatch(self.zk_path_type1)
        def watch_node(data, stat, event):
            if event and event.type == "CHANGED":
                print("TRAFFIC RE-ROUTED: {}".format(data))
                data, stat = self.zookeeper.get(self.zk_path_type1)
                self.port1 = data.decode('utf-8').split(',')[1]
                conn_str = "tcp://" + self.address + ":" + self.port1
                self.socket.connect(conn_str)
                print("Pulling data from: tcp://{}:{}".format(self.address, self.port1))

        @self.zookeeper.DataWatch(self.zk_path_type2)
        def watch_node(data, stat, event):
            if event and event.type == "CHANGED":
                print("TRAFFIC RE-ROUTED: {}".format(data))
                data, stat = self.zookeeper.get(self.zk_path_type2)
                self.port2 = data.decode('utf-8').split(',')[1]
                conn_str = "tcp://" + self.address + ":" + self.port2
                self.socket.connect(conn_str)
                print("Pulling data from: tcp://{}:{}".format(self.address, self.port2))
        for x in range(self.total_times_to_listen):
            self = filter_message(self)
            zipcode, temperature, sent_time = self.message.split(',')
            self.total_temp += int(temperature)
            self.listen_counter += 1
            _sent_dt = datetime.datetime.strptime(sent_time, "%m/%d/%Y %H:%M:%S.%f")
            self.output.append((datetime.datetime.utcnow() - _sent_dt).microseconds)

            if self.listen_counter == self.total_times_to_listen:
                print(self.print_message(self.total_temp / self.total_times_to_listen))
                self.get_published_history()
                self.context.destroy()

    def print_message(self, temperature):
        return "Average temperature for zipcode {} is: {}".format(self.zip_code, temperature)

    def get_published_history(self):
        for i in range(int(self.history_size)):
            message = receive_history(self, self.history_size)
            self.history_array.append(message)
        if len(self.history_array) != self.history_size:
            print("No History was received, history size sent by publisher is of a different size")
            return
        for i in range(len(self.history_array)):
            print("Temperature history is {}".format(self.history_array))


if __name__ == "__main__":
    print("Sysarg 1. Ip address, 2. zip code")
    address_type = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    topic = sys.argv[2] if len(sys.argv) > 2 else "10001"
    times_to_listen = sys.argv[3] if len(sys.argv) > 3 else 10
    requested_history = sys.argv[4] if len(sys.argv) > 4 else 10
    print(topic)
    sub = Subscriber(address_type, topic, times_to_listen, requested_history)
    sub.middleware_port_connection()
    sub.create_context()
    sub.get_message()
    print("Message Touch Times: {}".format(sub.output))
