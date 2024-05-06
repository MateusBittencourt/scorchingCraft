import threading
import time

# This class is a timer that runs asynchronously
class Timer(threading.Thread):
    # The constructor takes a timeout and a callback function
    def __init__(self, timeout, callback):
        super().__init__()
        self.timeout = timeout / 1000
        self.callback = callback
        self.stop_event = threading.Event()
        self.reset_event = threading.Event()

    # This function is the main loop of the timer
    def run(self):
        while not self.stop_event.is_set():
            # Wait for the timeout or until stop is called
            is_timeout = not self.reset_event.wait(self.timeout)
            if is_timeout:
                try:
                    self.callback()
                except Exception as e:
                    print(f"Error during callback execution: {e}")
                self.stop()
            else:
                # Reset was triggered, clear the event and continue
                self.reset_event.clear()

    # This function stops the timer
    def stop(self):
        self.stop_event.set()

    # This function resets the timer
    def reset(self):
        self.stop_event.clear()
        self.reset_event.set()
        self.run()