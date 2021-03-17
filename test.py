# import pytest
import os
import zmq
import unittest
from middleware import Broker
from publisher import Publisher
from subscriber import Subscriber


class TestMessaging(unittest.TestCase):
    def setUp(self):
        self.context = zmq.Context()
        self.frontend_socket = self.context.socket(zmq.XSUB)
        self.backend_socket = self.context.socket(zmq.XPUB)
        self.frontend_socket.bind(f"tcp://*:6665")
        self.backend_socket.bind(f"tcp://*:5556")

    def tearDown(self):
        self.context.destroy()

    def test_middleware_running_front(self):
        response = os.system("ping -c 1 tcp://*:6665")
        self.assertEqual(response, 0)

    def test_middleware_running_back(self):
        response = os.system("ping -c 1 tcp://*5556")
        self.assertEqual(response, 0)

    def test_publisher_attach_send(self):
        p = Publisher('localhost', '5556')
        p.context = zmq.Context()
        p.socket = p.context.socket(zmq.PUB)
        p.socket.connect(f"tcp://localhost:5556")
        # If the send fails, an exception is returned. None returned if successful
        self.assertEqual(p.socket.send_string("I can send!"), None)

    def test_subscriber_attach_rec(self):
        s = Subscriber('localhost', '10001', 10)
        s.context = zmq.Context()
        s.socket = s.context.socket(zmq.SUB)
        s.socket.connect(f"tcp://{s.address}:6665")
        self.assertEqual(s.socket.setsockopt_string(zmq.SUBSCRIBE, s.zip_code), None)


if __name__ == "__main__":
    unittest.main()