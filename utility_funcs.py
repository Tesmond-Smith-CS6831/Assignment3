# Utility functions for hiding ZMQ from the pub/sub
import zmq
import topic_hashtable


def register_pub():
    publisher_context = zmq.Context()
    publisher_socket = publisher_context.socket(zmq.PUB)
    return publisher_socket


def pub_send(publisher, message, proxy=2):
    if proxy == 1:
        publisher.socket.send_string(message)
    else:
        publisher.socket.connect(f"tcp://{publisher.host}:{publisher.port}")
        publisher.socket.send_string(message)


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