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

    def establish_broker(self):
        while True:
            self.context = zmq.Context()
            self.frontend_socket = self.context.socket(zmq.XSUB)
            self.backend_socket = self.context.socket(zmq.XPUB)
            self.frontend_socket.bind(f"tcp://*:{self.front}")
            self.backend_socket.bind(f"tcp://*:{self.back}")
            zmq.proxy(self.frontend_socket, self.backend_socket)


if __name__ == "__main__":
    # ip_address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    # print("Sysarg 1. Publisher connection port, 2. Subscriber connection port")
    socket_to_pub = sys.argv[1] if len(sys.argv) > 1 else "6663"
    socket_to_sub = sys.argv[2] if len(sys.argv) > 2 else "5556"
    broker = Broker(socket_to_pub, socket_to_sub)
    broker.establish_broker()
