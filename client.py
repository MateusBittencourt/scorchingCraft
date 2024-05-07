import time
import Pyro5.core
import Pyro5.api
import Pyro5.server
from threading import Thread
from dotmap import DotMap

class PyroClient(object):
    def __init__(self):
        self.name_server = Pyro5.api.locate_ns()
        self.daemon = Pyro5.api.Daemon()
        self.uri = self.daemon.register(self)
        self.name = self.uri.object

    def client_call_1(self):
        node_proxy = Pyro5.api.Proxy(self.get_leader())
        try:
            return node_proxy.client_call_1()
            # print(f"Sent event {event} to {leader}")
        except Pyro5.errors.PyroError as e:
            # print(f"Error sending vote to {leader}: {e}")
            pass
    
    def get_leader(self):
        uri_list = self.name_server.yplookup(meta_any={"Leader"})
        term = 0
        leader = None

        for key in uri_list.keys():
            key_term = int(key.split("-")[1])
            if key_term > term:
                term = key_term
                leader = uri_list[key]
        # print(f"Leader is {leader[0]}")
        return leader[0]

if __name__ == "__main__":
    print("Starting client")
    client = PyroClient()
    print("Calling client_call_1")
    client.client_call_1()