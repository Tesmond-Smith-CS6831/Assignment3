import sys
import datetime
import topic_hashtable
from random import randrange
from utility_funcs import register_pub, pub_send
from kazoo.client import KazooClient
import middleware


class Publisher:
    def __init__(self, host, zipcode, strength):
        self.socket = None
        self.port = None
        self.host = host
        self.zip_code = zipcode
        self.topic_ownership_strength = strength
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
                if pub_send(self, concat_message, self.topic_ownership_strength, how_to_publish) is False:
                    break

        else:
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
                zipcode = self.zip_code
                temperature = randrange(-80, 135)
                date_time = datetime.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S.%f")
                concat_message = str(zipcode) + "," + str(temperature) + "," + date_time
                if pub_send(self, concat_message, self.topic_ownership_strength) is False:
                    break
            topic_hashtable.reset_topic(zipcode)



# def update_topic_ownership_strength(index):
#     update_value = TOPIC_LIST_STRENGTH[index]
#     update_value += 1
#     TOPIC_LIST_STRENGTH.insert(index, update_value)


# def gen_publisher_nodes(number_of_nodes, host):
#     for i in range(number_of_nodes):
#         index = randrange(0, 4)
#         pub = Publisher(host, TOPIC_LIST[index], TOPIC_LIST_STRENGTH[index])
#         PUBLISHER_NODES.append(pub)
#         update_topic_ownership_strength(index)
#
#     for publisher in PUBLISHER_NODES:
#         publisher.initialize_context()
#         publisher.middleware_port_connection()
#         node_name = "{}{}".format(publisher.zk_path, "pub_node" + str(i))
#         if publisher.zookeeper.exists(node_name):
#             pass
#         else:
#             publisher.zookeeper.ensure_path(publisher.zk_path)
#             publisher.zookeeper.create(node_name)
#
#
# def publish_topics(how_to_publish):
#     while True:
#         for publisher in PUBLISHER_NODES:
#             if publisher.topic_ownership_strengh == 0:
#                 publisher.publish(how_to_publish)


if __name__ == "__main__":
    print("Sysarg 1. Ip Address, 2. Publisher functionality (i.e. 1. publish multiple topics, "
          "2. publish a singular topic: if 2 is selected enter zip code")
    address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    how_to_publish = sys.argv[2] if len(sys.argv) > 2 else 1
    topic = sys.argv[3] if len(sys.argv) > 3 else "10001"
    strength = sys.argv[4] if len(sys.argv) > 4 else 1
#     num_nodes = sys.argv[3] if len(sys.argv) > 3 else 5
    # gen_publisher_nodes()
    # publish_topics(how_to_publish)
    try:
        publisher = Publisher(address, topic, strength)
        publisher.initialize_context()
        publisher.middleware_port_connection()
        publisher.publish(int(how_to_publish))
    except KeyboardInterrupt:
        if topic and topic_hashtable.get_topic(topic):
            topic_hashtable.reset_topic(topic)
            print("removed topic record")


