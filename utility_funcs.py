# Utility functions for hiding ZMQ from the pub/sub
import zmq
import topic_hashtable

def register_pub():
    publisher_context = zmq.Context()
    publisher_socket = publisher_context.socket(zmq.PUB)
    return publisher_socket


def pub_send(publisher, message, strength, proxy=None):
    zipcode, temperature, date_time = message.split(',')
    if proxy == 1:
        if topic_hashtable.get_topic(zipcode):
            cur_topic_strength = topic_hashtable.get_topic(zipcode)[0][0]
            if strength < cur_topic_strength:
                topic_hashtable.set_topic(zipcode, message, strength)
                publisher.socket.send_string(message)
            else:
                print("Lower strength, will not send. Current strength:{}".format(cur_topic_strength))
        else:
            topic_hashtable.set_topic(message)
    else:
        if topic_hashtable.get_topic(zipcode):
            cur_topic_strength = topic_hashtable.get_topic(zipcode)[0][0]
            if strength < cur_topic_strength:
                topic_hashtable.set_topic(zipcode, message, strength)
                publisher.socket.connect(f"tcp://{publisher.host}:{publisher.port}")
                publisher.socket.send_string("{},{},{},{}".format(zipcode, temperature, date_time, strength))
            else:
                print("Lower strength, will not send. Current strength:{}".format(cur_topic_strength))
        else:
            topic_hashtable.set_topic(message)
            publisher.socket.connect(f"tcp://{publisher.host}:{publisher.port}")
            publisher.socket.send_string("{},{},{},{}".format(zipcode, temperature, date_time, strength))


def register_sub(subscriber):
    subscriber.context = zmq.Context()
    subscriber.socket = subscriber.context.socket(zmq.SUB)
    subscriber.socket.connect(f"tcp://{subscriber.address}:{subscriber.port}")
    subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
    return subscriber


def filter_message(subscriber):
    subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
    subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
    subscriber.message = subscriber.socket.recv_string()
    return subscriber