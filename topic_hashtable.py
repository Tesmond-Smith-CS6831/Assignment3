TOPIC_HASH = {}


def get_topic(topic):
    if TOPIC_HASH.get(topic, ):
        return [topic, TOPIC_HASH[topic]]


def set_topic(topic):
    if TOPIC_HASH.get(topic) is None:
        TOPIC_HASH[topic] = 1
    else:
        TOPIC_HASH[topic] += 1
    return [topic, TOPIC_HASH[topic]]


def decrement_topic(topic):
    if TOPIC_HASH.get(topic) is not None and TOPIC_HASH[topic] > 0:
        TOPIC_HASH[topic] -= 1
        return [topic, TOPIC_HASH[topic]]
    elif TOPIC_HASH.get(topic) is not None:
        return [topic, TOPIC_HASH[topic]]
    else:
        return "Topic not found :("