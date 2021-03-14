

PPP_INIT = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat
PPP_DISCONNECT = b"\x03"  # Disconnect
PPP_READY = b"\x04"  # Signals worker is not ready
PPP_NOT_READY = b"\x05"  # Signals worker is not ready
PPP_DONE = b"\x06"
PPP_EMPTY = b"\x07"
PPP_MSG = b"\x08"
PPP_DONE_DISCONNECT = b"\x09"
