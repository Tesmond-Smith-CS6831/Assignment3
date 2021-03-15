import zmq


def register_pub(publisher_obj):
    publisher_context = zmq.Context()
    publisher_socket = publisher_context.socket(zmq.PUB)
    # publisher.socket.connect(f"tcp://{publisher.host}:{publisher.port}")
    return publisher_socket


def pub_send(publisher, message, proxy = 2):
    zipcode, temperature, date_time = message.split(',')
    self.proxy = proxy
    if proxy == 1:
        publisher.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))
    else:
        self.tempPubPort = int(publisher.port) + 1
        publisher.socket.connect(f"tcp://{publisher.host}:{self.tempPubPort}")
        publisher.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))