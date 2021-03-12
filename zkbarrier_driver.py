import os
import sys
import argparse
import logging
from kazoo.client import KazooClient
from zkbarrier_app import AppThread

logging.basicConfig()


def thread_func(app):
    """A thread function to be executed by the client app threads"""

    @app.zk.DataWatch(app.ppath)
    def data_change_watcher(data, stat):
        """Data Change Watcher"""
        print(("AppThread::DataChangeWatcher - data = {}, stat = {}".format(data, stat)))
        val = int(data)
        if val == app.cond:
            print(("AppThread: {}, barrier is reached".format(app.name)))

    while not app.barrier:
        print(("AppThread {} barrier not reached yet".format(app.name)))
        if app.zk.exists(app.ppath):
            value, stat = app.zk.get(app.ppath)
            print(("AppThread {} found parent znode value = {}, stat = {}".format(app.name, value, stat)))
        else:
            print(("{} znode does not exist yet (strange)".format(app.ppath)))

    print(("AppThread {} has reached the barrier and so we disconnect from zookeeper".format(app.name)))
    app.zk.stop()
    app.zk.close()

    print(("AppThread {}: Bye Bye ".format(app.name)))


class ZK_Driver:
    """ The ZooKeeper Driver Class """

    def __init__(self, args):
        self.zkIPAddr = args.zkIPAddr
        self.zkPort = args.zkPort
        self.numClients = args.numClients
        self.zk = None
        self.path = "/barrier"
        self.threads = []

    def dump(self):
        """dump contents"""
        print("=================================")
        print(("Server IP: {}, Port: {}; Path = {}, NumClients = {}".format(self.zkIPAddr, self.zkPort, self.path,
                                                                            self.numClients)))
        print("=================================")

    def init_driver(self):
        """Initialize the client driver program"""

        try:
            self.dump()
            hosts = self.zkIPAddr + str(":") + str(self.zkPort)
            print(("Driver::init_driver -- instantiate zk obj: hosts = {}".format(hosts)))
            self.zk = KazooClient(hosts)
            print(("Driver::init_driver -- state = {}".format(self.zk.state)))

        except:
            print("Unexpected error in init_driver:", sys.exc_info()[0])
            raise

    def run_driver(self):
        import pdb;pdb.set_trace()
        """The actual logic of the driver program """
        print("Driver::run_driver -- connect with server")
        self.zk.start()
        print(("Driver::run_driver -- state = {}".format(self.zk.state)))
        print("Driver::run_driver -- create a znode for barrier")
        try:
            self.zk.create(self.path, value=b"0")
        except:
            pass

        @self.zk.ChildrenWatch(self.path)
        def child_change_watcher(children):
            """Children Watcher"""
            print(("Driver::run -- children watcher: num childs = {}".format(len(children))))
            if self.zk.exists(self.path):
                print(("Driver::child_change_watcher - setting new value for children = {}".format(len(children))))
                self.zk.set(self.path, bytes(str(len(children)), 'utf-8'))
            else:
                print("Driver:run_driver -- child watcher -- znode does not exist")

        print("Driver::run_driver -- start the client app threads")
        thread_args = {'server': self.zkIPAddr, 'port': self.zkPort, 'ppath': self.path, 'cond': self.numClients}

        for i in range(self.numClients):
            thr_name = "Thread" + str(i)
            t = AppThread(thr_name, thread_func, thread_args)
            self.threads.append(t)
            t.start()

        print("Driver::run_driver -- wait for the client app threads to terminate")
        for i in range(self.numClients):
            self.threads[i].join()

        print(("Driver::run_driver -- now remove the znode {}".format(self.path)))
        self.zk.delete(self.path, recursive=True)

        print("Driver::run_driver -- disconnect and close")
        self.zk.stop()
        self.zk.close()

        print("Driver::run_driver -- Bye Bye")


def parseCmdLineArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--zkIPAddr", default="127.0.0.1", help="ZooKeeper server ip address, default 127.0.0.1")
    parser.add_argument("-c", "--numClients", type=int, default=5,
                        help="Number of client apps in the barrier, default 5")
    parser.add_argument("-p", "--zkPort", type=int, default=2181, help="ZooKeeper server port, default 2181")
    args = parser.parse_args()
    return args


def main():
    """ Main program """

    print("Demo program for ZooKeeper-based Barrier Sync")
    parsed_args = parseCmdLineArgs()
    driver = ZK_Driver(parsed_args)
    driver.init_driver()
    driver.run_driver()


if __name__ == '__main__':
    main()
