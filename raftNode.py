import Pyro5.core
import Pyro5.api
import Pyro5.server
from threading import Thread
from multicast import MulticastService
from dotmap import DotMap
from stateMachine import StateMachine
from typeEnums import MessageType, EventType

############################################################################################
# Class that represents the Pyro5 node
@Pyro5.api.expose
class RaftNode(object):
    def __init__(self):
        self.votes = 0
        self.log = []
        
        self.leader = None
        self.term = 0
        self.nodes = []
        
        self.name_server = Pyro5.api.locate_ns()
        
        
        self.daemon = Pyro5.api.Daemon()
        self.uri = self.daemon.register(self)
        self.name = self.uri.object
                
        # Start the multicast listener on a separate thread
        self.multicast_service = MulticastService(self.update_nodes)
        self.receive_thread = Thread(target=self.multicast_service.receive_multicast)
        self.receive_thread.daemon = True
        self.receive_thread.start()

        # Send node info using multicast service
        self.multicast_service.send_multicast({"data": {"name": self.name, "uri": str(self.uri)}, "eventType": "new_node"})
        
        # Initialize the state machine
        self.init_state()
        
        # Start the daemon loop, listening for incoming requests
        self.daemon.requestLoop()
        
    # This function is called when a new event is received
    def on_event(self, event):
        # print(f"{self.name} received event {event}")
        event = DotMap(event)
        event.type = EventType(event.type)
        event.messageType = MessageType(event.messageType)
        self.state = self.state.on_event(event)
        return None
        
    def init_state(self):
        self.state = StateMachine(self)
        self.state.init_class()

    def current_state(self):
        return self.state
    
    def update_nodes(self, reply):
        # print(f"Received node list update: {reply}")
        reply = DotMap(reply)
        if reply.data not in self.nodes and reply.data.name != self.name:
            # print(f"Adding node {reply.data.name} to list")
            self.nodes.append(reply.data)
        if reply.eventType == "new_node" and reply.data.name != self.name:
            self.multicast_service.send_multicast({"data": {"name": self.name, "uri": str(self.uri)}, "eventType": "update_nodes"})
    
    def proxy_call(self, uri, event):
        node_proxy = Pyro5.api.Proxy(uri)
        try:
            node_proxy.on_event(event)
            # print(f"Sent event {event} to {uri}")
        except Pyro5.errors.PyroError as e:
            # print(f"Error sending vote to {uri}: {e}")
            pass
    

if __name__ == "__main__":
    node = RaftNode()