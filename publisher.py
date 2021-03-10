# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code taken from ZeroMQ examples with additional
# comments or extra statements added to make the code
# more self-explanatory  or tweak it for our purposes
#
# We are executing these samples on a Mininet-emulated environment
#
#

#
#   Weather update server
#   Binds PUB socket to tcp://*:6663 or whatever system input socket is
#   Publishes random weather updates
#
import sys
import zmq
import datetime
from random import randrange

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

class Publisher:
    def __init__(self, port_to_bind, host, zipcode):
        self.context = None
        self.socket = None
        self.port = port_to_bind
        self.host = host
        self.zip_code = zipcode

    def initialize_context(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect(f"tcp://{self.host}:{self.port}")

    def publish(self, how_to_publish):
        if how_to_publish == 1:
            while True:
                zipcode = randrange(1, 100000)
                temperature = randrange(-80, 135)
                date_time = datetime.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S.%f")
                self.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))
        else:
            while True:
                zipcode = self.zip_code
                temperature = randrange(-80, 135)
                date_time = datetime.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S.%f")
                self.socket.send_string("{},{},{}".format(zipcode, temperature, date_time))


if __name__ == "__main__":
    print("Sysarg 1. Ip Address, 2. Port number, 3. Publisher functionality (i.e. 1. publish multiple topics, "
          "2. publish a singular topic: if 2 is selected enter zip code")
    address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port_to_bind = sys.argv[2] if len(sys.argv) > 2 else "6663"
    how_to_publish = sys.argv[3] if len(sys.argv) > 3 else 1
    topic = sys.argv[4] if len(sys.argv) > 4 else "10001"
    publisher = Publisher(port_to_bind, address, topic)
    publisher.initialize_context()
    publisher.publish(how_to_publish)


