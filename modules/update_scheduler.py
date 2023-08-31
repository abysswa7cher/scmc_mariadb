from datetime import datetime

class UpdateScheduler():
    def __init__(self, timeout, db, host):
        self.db = db
        self.host = host
        self.timeout = timeout*60
        self.last_timestamp = None
        self._read_last_timestamp()
        self._update_current_timestamp()
        self.update_time_delta = self.current_timestamp - self.last_timestamp

        print(f'''Last update was on {self.last_update_date}\nTime since last update: {round(self.update_time_delta/60, 1) if self.update_time_delta > 1 else self.update_time_delta} {"min" if self.update_time_delta > 1 else "sec"}.''')

    def _update_current_timestamp(self):
        self.current_timestamp = datetime.now().timestamp()
    
    def _read_last_timestamp(self):
        try:
            if not self.db.connected:
                self.db.connect(self.host)

            last_update = self.db.get_last_row("update_log")

            self.last_timestamp = datetime.timestamp(last_update[0])
            self.last_update_date = datetime.fromtimestamp(self.last_timestamp)
        except Exception as e:
            self.last_timestamp = -1
            self.last_update_date = -1
            raise
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
        if not self.db.connected:
            self.db.connect(self.host)
            
        self.last_update_date = datetime.now()
        self.db.insert("update_log", ["timestamp"], [self.last_update_date])
        print(f"Last update saved as: {self.last_update_date}")