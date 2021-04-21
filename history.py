import sys
import zmq


class History:

    def __init__(self):
        self.history = {}
        self.front_end_socket = None
        self.back_end_socket = None
        self.context = None
        self.front_end_port = 2222
        self.back_end_port = 2223

    def initialize_history(self):
        self.context = zmq.Context()
        self.front_end_socket = self.context.socket(zmq.XSUB)
        self.back_end_socket = self.context.socket(zmq.XPUB)
        self.front_end_socket.bind(f"tcp://*:{self.front_end_port}")
        self.back_end_socket.bind(f"tcp://*:{self.back_end_port}")

    def maintain_history(self):
        zmq.proxy(self.front_end_socket, self.back_end_socket)


if __name__ == "__main__":
    history_node = History()
    history_node.initialize_history()
    history_node.maintain_history()
