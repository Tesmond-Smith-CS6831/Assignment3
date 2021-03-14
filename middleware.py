"""
Initial thoughts from Rick T:
    - creating anonymity is the goal
    - middleware could act as a hashtable with initialized buckets 'n' for number of zips
        - key: zipcode
        - value: weather stats
    - Publisher script calls into middleware hashtable to update value using ziptable[zip] = collected_weather
    - Subscriber script polls value from middleware using ziptable[zip] or ziptable.get(zip)

    - this allows for neither the client/server knowing the pub/sub servers
"""
import sys
import zmq


class Broker:

    def __init__(self, frontend_port, backend_port):
        self.front = frontend_port
        self.back = backend_port
        self.frontend_socket = None
        self.backend_socket = None
        self.context = None
        self.proxy = None
        self.tempPubPort = None

    def establish_broker(self):
        while True:
            self.context = zmq.Context()
            self.frontend_socket = self.context.socket(zmq.XSUB)
            self.backend_socket = self.context.socket(zmq.XPUB)
            self.frontend_socket.bind(f"tcp://*:{self.front}")
            self.backend_socket.bind(f"tcp://*:{self.back}")
            zmq.proxy(self.frontend_socket, self.backend_socket)

    def register_pub(self, publisher):
        publisher.context = zmq.Context()
        publisher.socket = publisher.context.socket(zmq.PUB)
        publisher.socket.connect(f"tcp://{publisher.host}:{publisher.port}")
        return publisher

    def pub_send(self, publisher, message, proxy = 2):
        zipcode, temperature, date_time = message.split(',')
        self.proxy = proxy
        if proxy == 1:
            publisher.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))
        else:
            self.tempPubPort = int(publisher.port) + 1
            publisher.socket.connect(f"tcp://{publisher.host}:{self.tempPubPort}")
            publisher.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))

    def register_sub(self, subscriber):
        subscriber.context = zmq.Context()
        subscriber.socket = subscriber.context.socket(zmq.SUB)
        subscriber.socket.connect(f"tcp://{subscriber.address}:{subscriber.port}")
        subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        return subscriber

    def filter_message(self, subscriber):
        if self.proxy == 2:
            subscriber.socket.connect(f"tcp://{subscriber.address}:{self.tempPubPort}")
            subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        else:
            subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
            subscriber.socket.setsockopt_string(zmq.SUBSCRIBE, subscriber.zip_code)
        subscriber.message = subscriber.socket.recv_string()
        return subscriber


socket_to_pub = sys.argv[1] if len(sys.argv) > 1 else "6663"
socket_to_sub = sys.argv[2] if len(sys.argv) > 2 else "5556"
universal_broker = Broker(socket_to_pub, socket_to_sub)

if __name__ == "__main__":
    universal_broker.establish_broker()
