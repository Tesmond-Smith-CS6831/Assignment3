# Since ownership strength is reverse magnitude, highest strength is 1
# New topics have strength given, if current strength > new strength, new strength is current information
from kazoo.client import KazooClient

zookeeper = KazooClient(hosts='127.0.0.1:2181')
zookeeper.start()


def init_topichash(topic):
    topic_path = "/topics/"
    topic_node = "/topics/{}".format(topic)
    strength = bytes("{}".format("1").encode("utf-8"))
    if zookeeper.exists(topic_node):
        pass
    else:
        zookeeper.ensure_path(topic_path)
        zookeeper.create(topic_node, strength)


def get_topic(topic):
    topic_node = "/topics/{}".format(topic)
    if zookeeper.exists(topic_node):
        data, stat = zookeeper.get(topic_node)
        current_strength = int(data.decode('utf-8'))
        return (topic, current_strength)
    else:
        return False


def set_topic(topic):
    topic_node = "/topics/{}".format(topic)
    strength = None
    if not get_topic(topic):
        init_topichash(topic)
        strength = get_topic(topic)[1]
    else:
        cur_strength = get_topic(topic)[1]
        cur_strength += 1
        strength = bytes("{}".format(str(cur_strength)).encode("utf-8"))
        zookeeper.set(topic_node, strength)
    print("Strength adjusted for set: {}".format(strength))
    return (topic, strength)


def decrement_topic(topic):
    topic_node = "/topics/{}".format(topic)
    if get_topic(topic) is not None and get_topic(topic)[1] > 1:
        cur_strength = get_topic(topic)[1]
        cur_strength -= 1
        new_strength = bytes("{}".format(str(cur_strength)).encode("utf-8"))
        zookeeper.set(topic_node, new_strength)
        print("Topic strength adjusted for pub removal")
    elif get_topic(topic) is not None:
        print("Topic strength adjusted for pub removal, set to 1")
    else:
        print("Topic not found :(")