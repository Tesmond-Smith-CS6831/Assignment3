# Since ownership strength is reverse magnitude, highest strength is 1
# New topics have strength given, if current strength > new strength, new strength is current information
TOPIC_HASH = {}


def get_topic(topic):
    if TOPIC_HASH.get(topic) is not None:
        return [topic, TOPIC_HASH[topic]]
    return False


def set_topic(topic, info, strength):
    if TOPIC_HASH.get(topic) is None:
        TOPIC_HASH[topic] = (strength, info)
    elif TOPIC_HASH.get(topic) is not None and TOPIC_HASH[topic] > strength:
        TOPIC_HASH[topic] = (strength, info)
    return [topic, TOPIC_HASH[topic]]