import threading
import time

class Timer:
    def __init__(self, timeout, callback, continuous=False):
        self.timeout = timeout / 1000  # convert ms to seconds
        self.callback = callback
        self.continuous = continuous
        self.timer_thread = None
        self.stop_event = threading.Event()
        self.reset_event = threading.Event()

    def _timer_loop(self):
        while not self.stop_event.is_set():
            if self.reset_event.wait(self.timeout):
                # If reset_event is set, clear it for the next iteration
                self.reset_event.clear()
            else:
                # Timeout happened
                if not self.stop_event.is_set():  # Double-check to avoid race condition
                    try:
                        self.callback()
                    except Exception as e:
                        print(f"Error during callback execution: {e}")
                    if not self.continuous:
                        break  # Exit the loop if not continuous
        self.timer_thread = None  # Thread has finished, clear the reference

    def start(self):
        if self.timer_thread is not None:
            # Timer is already running
            return
        self.stop_event.clear()  # Ensure stop_event is clear at start
        self.reset_event.clear()  # Ensure reset_event is clear at start
        self.timer_thread = threading.Thread(target=self._timer_loop)
        self.timer_thread.start()

    def stop(self):
        self.stop_event.set()
        # if self.timer_thread is not None:
        #     self.timer_thread.join()  # Wait for the thread to finish

    def reset(self):
        self.reset_event.set()