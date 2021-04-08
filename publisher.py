import sys
import datetime
import topic_hashtable
from random import randrange
from utility_funcs import register_pub, pub_send, send_history
from kazoo.client import KazooClient
import middleware


class Publisher:
    def __init__(self, host, zipcode, history_number, history = 2222):
        self.socket = None
        self.port = None
        self.host = host
        self.history_to_keep = int(history_number)
        self.published_history = []
        self.history_socket = None
        self.history_port = history
        self.zip_code = zipcode
        self.topic_ownership_path = "/topics/{}".format(zipcode)
        self.zookeeper = KazooClient(hosts='127.0.0.1:2181')
        self.zk_path = '/leader/leadNode'
        self.zookeeper.start()

    def initialize_context(self):
        self.socket = register_pub()

    def middleware_port_connection(self):
        self = middleware.publish_node_conn(self)

    def publish(self, how_to_publish):
        if how_to_publish == 1:
            print("Sending Data to: tcp://{}:{}".format(self.host, self.port))

            @self.zookeeper.DataWatch(self.zk_path)
            def watch_node(data, stat, event):
                if event and event.type == "CHANGED":
                    print("data changed: {}".format(data))
                    data, stat = self.zookeeper.get(self.zk_path)
                    self.port = data.decode('utf-8').split(',')[0]
                    conn_str = "tcp://" + self.host + ":" + self.port
                    self.socket.connect(conn_str)
                    print("Sending Data to: tcp://{}:{}".format(self.host, self.port))

            while True:
                zipcode = randrange(1, 100000)
                temperature = randrange(-80, 135)
                date_time = datetime.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S.%f")
                concat_message = str(zipcode) + "," + str(temperature) + "," + date_time
                self.update_published_history(concat_message)
                if pub_send(self, concat_message, how_to_publish) is False:
                    break
                for i in range(len(self.published_history)):
                    new_message = str(zipcode) + "," + str(temperature) + "," + str(self.history_to_keep)
                    send_history(self, new_message)

        else:
            print("Sending Data to: tcp://{}:{}".format(self.host, self.port))
            self.check_ownership()

            @self.zookeeper.DataWatch(self.zk_path)
            def watch_node(data, stat, event):
                if event and event.type == "CHANGED":
                    print("data changed: {}".format(data))
                    data, stat = self.zookeeper.get(self.zk_path)
                    self.port = data.decode('utf-8').split(',')[0]
                    conn_str = "tcp://" + self.host + ":" + self.port
                    self.socket.connect(conn_str)
                    print("Sending Data to: tcp://{}:{}".format(self.host, self.port))

            @self.zookeeper.DataWatch(self.topic_ownership_path)
            def watch_node(data, stat, event):
                if event and event.type == "CHANGED":
                    print("Ownership changed!")

            while True:
                zipcode = self.zip_code
                temperature = randrange(-80, 135)
                date_time = datetime.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S.%f")
                concat_message = str(zipcode) + "," + str(temperature) + "," + date_time
                pub_send(self, concat_message)
                for i in range(len(self.published_history)):
                    new_message = str(zipcode) + "," + str(temperature) + "," + str(self.history_to_keep)
                    send_history(self, new_message)

    def update_published_history(self, message):
        if len(self.published_history) < int(self.history_to_keep):
            self.published_history.append(message)

        else:
            if len(self.published_history) == self.history_to_keep:
                new_list = []
                for i in range(self.history_to_keep):
                    if i + 1 == self.history_to_keep:
                        new_list.append(message)
                    else:
                        new_list.append(self.published_history[i + 1])
                self.published_history = new_list

    def check_ownership(self):
        if topic_hashtable.get_topic(self.zip_code):
            cur_topic_strength = topic_hashtable.get_topic(self.zip_code)[1]
            if cur_topic_strength > 1:
                print("***Node with higher ownership level. Current topic strength:{}. SUSPENDING".format(
                    cur_topic_strength))
            else:
                topic_hashtable.set_topic(self.zip_code)
        else:
            topic_hashtable.set_topic(self.zip_code)


if __name__ == "__main__":
    print("Sysarg 1. Ip Address, 2. Publisher functionality (i.e. 1. publish multiple topics, "
          "2. publish a singular topic: if 2 is selected enter zip code")
    address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    how_to_publish = sys.argv[2] if len(sys.argv) > 2 else 1
    topic = sys.argv[3] if len(sys.argv) > 3 else "10001"
    history = sys.argv[4] if len(sys.argv) > 4 else 10
    try:
        publisher = Publisher(address, topic, history)
        publisher.initialize_context()
        publisher.middleware_port_connection()
        publisher.publish(int(how_to_publish))
    except KeyboardInterrupt:
        if topic and topic_hashtable.get_topic(topic):
            topic_hashtable.decrement_topic(topic)
