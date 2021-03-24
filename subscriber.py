import sys
import datetime
import middleware
from utility_funcs import register_sub, filter_message
from kazoo.client import KazooClient


class Subscriber:
    def __init__(self, address, topic, time_to_listen):
        self.address = address
        self.port = None
        self.total_temp = 0
        self.zip_code = topic
        self.total_times_to_listen = int(time_to_listen)
        self.listen_counter = 0
        self.message = None
        self.context = None
        self.socket = None
        self.output = []
        self.zookeeper = KazooClient(hosts='127.0.0.1:2181')
        self.zk_path = '/leader/leadNode'
        self.zookeeper.start()

    def create_context(self):
        self = register_sub(self)

    def middleware_port_connection(self):
        self = middleware.subscribe_node_conn(self)

    def get_message(self):
        print("Pulling data from: tcp://{}:{}".format(self.address, self.port))

        @self.zookeeper.DataWatch(self.zk_path)
        def watch_node(data, stat, event):
            if event and event.type == "CHANGED":
                print("data changed: {}".format(data))
                data, stat = self.zookeeper.get(self.zk_path)
                self.port = data.decode('utf-8').split(',')[1]
                conn_str = "tcp://" + self.address + ":" + self.port
                self.socket.connect(conn_str)
                print("Pulling data from: tcp://{}:{}".format(self.address, self.port))
        for x in range(self.total_times_to_listen):
            self = filter_message(self)
            zipcode, temperature, sent_time = self.message.split(',')
            self.total_temp += int(temperature)
            self.listen_counter += 1
            _sent_dt = datetime.datetime.strptime(sent_time, "%m/%d/%Y %H:%M:%S.%f")
            self.output.append((datetime.datetime.utcnow() - _sent_dt).microseconds)

            if self.listen_counter == self.total_times_to_listen:
                print(self.print_message(self.total_temp / self.total_times_to_listen))
                self.context.destroy()

    def print_message(self, temperature):
        return "Average temperature for zipcode {} is: {}".format(self.zip_code, temperature)


if __name__ == "__main__":
    print("Sysarg 1. Ip address, 2. zip code")
    address_type = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    topic = sys.argv[2] if len(sys.argv) > 2 else "10001"
    times_to_listen = sys.argv[3] if len(sys.argv) > 3 else 10
    print(topic)
    sub = Subscriber(address_type, topic, times_to_listen)
    sub.middleware_port_connection()
    sub.create_context()
    sub.get_message()
    print("Message Touch Times: {}".format(sub.output))
