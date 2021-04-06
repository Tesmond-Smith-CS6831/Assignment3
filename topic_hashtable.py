# Since ownership strength is reverse magnitude, highest strength is 1
# New topics have strength given, if current strength > new strength, new strength is current information
import pickle
import os


def get_topic(topic=None):
    if os.path.getsize('pkl_hash/topichash.pkl') > 0:
        with open('pkl_hash/topichash.pkl', 'rb') as pickle_in:
            topic_hash = pickle.load(pickle_in)
            if not topic_hash:
                topic_hash = {}
                return topic_hash
            elif topic_hash.get(topic) is not None:
                return [topic, topic_hash[topic]]
            else:
                return topic_hash
    return {}


def set_topic(topic, info, strength):
    topic_hash = get_topic()
    if len(get_topic(topic)) < 1:
        topic_hash[topic] = (strength, info)
    elif get_topic(topic) is not None and get_topic(topic)[1][0] > strength:
        topic_hash[topic] = (strength, info)

    with open('pkl_hash/topichash.pkl', 'wb') as pickle_out:
        pickle.dump(topic_hash, pickle_out)
    return [topic, topic_hash[topic]]

def reset_topic(topic):
    topic_hash = get_topic()
    with open('pkl_hash/topichash.pkl', 'wb') as pickle_out:
        del topic_hash[topic]
        pickle.dump(topic_hash, pickle_out)