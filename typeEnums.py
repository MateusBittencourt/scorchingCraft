from enum import Enum

class EventType(Enum):
    HEARTBEAT = 1
    VOTE = 2
    ROLL_CALL = 3
    APPEND_ENTRIES = 4
    COMMIT_ENTRIES = 5
    TIMEOUT = 6
    INIT = 7

class MessageType(Enum):
    REPLY = 1
    EVENT = 2