# Utility functions for hiding ZMQ from the pub/sub
import zmq
import topic_hashtable


def register_pub():
    publisher_context = zmq.Context()
    publisher_socket = publisher_context.socket(zmq.PUB)
    return publisher_socket


def pub_send(publisher, message, proxy=2):
    zipcode, temperature, date_time = message.split(',')
    if proxy == 1:
        publisher.socket.send_string(message)
    else:
        publisher.socket.connect(f"tcp://{publisher.host}:{publisher.port}")
        publisher.socket.send_string(message)


def register_sub(subscriber):
    subscriber.context = zmq.Context()
    subscriber.socket = subscriber.context.socket(zmq.SUB)
    subscriber.history_socket = subscriber.context.socket(zmq.SUB)
    subscriber.socket.connect(f"tcp://{subscriber.address}:{subscriber.port}")
    subscriber.history_socket.connect(f"tcp://{subscriber.address}:{subscriber.history_port}")
    subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
    subscriber.history_socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
    return subscriber


def filter_message(subscriber):
    subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
    subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
    subscriber.message = subscriber.socket.recv_string()
    return subscriber


def send_history(publisher, message):
    zipcode, temp, size = message.split(",")
    publisher.history_socket = zmq.Context().socket(zmq.PUB)
    publisher.history_socket.connect(f"tcp://{publisher.host}:{publisher.history_port}")
    publisher.history_socket.send_string("{},{},{}".format(zipcode, temp, size))


def receive_history(subscriber, size):
    subscriber.history_socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
    message = subscriber.socket.recv_string()
    zipcode, topic_temp, topic_size = message.split(",")
    # print(zipcode + ", " + topic_temp + ", " + topic_size)
    # if topic_size != size:
    #     return None
    # else:
    return topic_temp
