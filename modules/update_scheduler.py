from datetime import datetime, date
import os
import pandas as pd


class UpdateScheduler():
    def __init__(self, timeout):
        self.timeout = timeout*60
        self.today = str(date.today())
        self._read_last_timestamp()
        self._update_current_timestamp()
        self.update_time_delta = self.current_timestamp - self.last_timestamp

        print(f'''Last update was on {self.last_update}\nTime since last update: {round(self.update_time_delta/60, 1) if self.update_time_delta > 1 else self.update_time_delta} {"min" if self.update_time_delta > 1 else "sec"}.''')

    def _update_current_timestamp(self):
        self.current_timestamp = datetime.now().timestamp()
    
    def _read_last_timestamp(self):
        try:
            df = pd.read_csv("data/update_log.csv").iloc[-1]
            self.last_timestamp = float(df.values[0])
            self.last_update = datetime.fromtimestamp(self.last_timestamp)
        except Exception as e:
            print(e)
            with open("data/update_log.csv", "w+") as f:
                f.write("timestamp,date\n0,0\n")
            self.last_timestamp = -1
            self.last_update = -1

    def update(self):
        self._update_current_timestamp()
        self._read_last_timestamp()
        
        if self.last_timestamp == -1:
            self.last_timestamp = self.current_timestamp
        self.update_time_delta = self.current_timestamp - self.last_timestamp

        seconds_until_update = self.timeout - self.update_time_delta
        if seconds_until_update > 1:
            print(f"\033[2KTime until next update: {int(self.timeout - self.update_time_delta)}s.", end="\r", flush=True)
        elif 0 < seconds_until_update <= 1:
            print(f"\033[2KTime until next update: {int((self.timeout - self.update_time_delta)*100)}ms.", end="\r", flush=True)
        elif seconds_until_update <= 0:
            print(f"\033[2KTime until next update: {int((self.timeout - self.update_time_delta)*100)}s.", end="\r", flush=True)
        
        if self.update_time_delta >= self.timeout:
            return True
        else: 
            return False
        
    def log_last_update(self):
        self.last_update = datetime.now()
        log = {"timestamp": self.current_timestamp, "date": self.last_update}
        df = pd.DataFrame([log], index=None)
        df.to_csv("data/update_log.csv", mode='a', index=False, header=False)
        print(f"Last update saved as: {self.last_update}")