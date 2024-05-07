from enum import Enum

class EventType(Enum):
    HEARTBEAT = 1
    VOTE = 2
    TIMEOUT = 3
    APPEND_ENTRIES = 4
    COMMIT_ENTRIES = 5
    
class MessageType(Enum):
    REPLY = 1
    EVENT = 2