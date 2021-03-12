import os
import sys
import threading
from kazoo.client import KazooClient
import logging

logging.basicConfig()


class AppThread(threading.Thread):
    def __init__(self, name, func, args):
        threading.Thread.__init__(self, name=name)
        self.zkIPAddr = args['server']
        self.zkPort = args['port']
        self.cond = args['cond']
        self.ppath = args['ppath']
        self.zk = None
        self.func = func
        self.barrier = False

    def run(self):
        try:
            print(("AppThread::run - Client {} now running and opening connection to zookeeper".format(self.name)))
            hosts = self.zkIPAddr + str(":") + str(self.zkPort)
            self.zk = KazooClient(hosts)
            print(("App::run -- state = {}".format(self.zk.state)))
            self.zk.start()

            while True:
                if self.zk.exists(self.ppath):
                    print("AppThread::run - parent znode is set")
                    # in that case we create our child node
                    self.zk.create(self.ppath + str("/") + self.name, value=bytes(self.name, 'utf-8'), ephemeral=True,
                                   makepath=True)
                    break
            else:
                print("AppThread::run -- parent znode is not yet up")
            self.func(self)

        except:
            print("Unexpected error in AppThread::run", sys.exc_info()[0])
            raise
