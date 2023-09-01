# Module Imports
import argparse
import warnings
import os, sys
import modules.update_scheduler as us
import multiprocessing
from datetime import datetime
from threading import Thread
from modules.db_module import MarketDB
from modules.fetch_market_data import fetch_market_data
from modules.fetch_resource_reference import fetch_resource_reference
from modules.parse_table_to_chart import draw_chart
warnings.filterwarnings('ignore')

parser = argparse.ArgumentParser(description='')
parser.add_argument("-U",
                    "--update_timeout",
                    nargs='?',
                    const=1,
                    default=10,
                    help="Sets the amount of minutes to pass between updates.",
                    type=float)

parser.add_argument("-H",
                    "--host",
                    nargs='?',
                    const=1,
                    default="mariadb",
                    help="Sets the host to connect to from database.ini.",
                    type=str)

args = parser.parse_args()

def update_and_post_data_to_db():
  DB = MarketDB()
  # DB.connect(args.host)#args.host
  update_scheduler = us.UpdateScheduler(args.update_timeout, DB, args.host)
  
  if not os.path.isfile("data/resources.csv"):
    print("Resources reference file not found, loading a new one...")
    try:
      fetch_resource_reference()
    except Exception as e:
      print(e)
  
  while True:
    
    if update_scheduler.update():
        print("\033[2KUpdating and posting to DB...", end="\r", flush=True)
        
        try:
          fetch_market_data(DB)
          update_scheduler.log_last_update()
          # job = multiprocessing.Process(target=draw_chart(DB, table_name="crude_oil", date=datetime.now().strftime("%Y-%m-%d")),args=())
          # job.start()
        except Exception as e:
          print(e)
          sys.exit(1)

def new_mplthread(func):
  ts = [Thread(target=func)]
  for t in ts:
    t.start()
  for t in ts:
    t.join()
    print(f"thread is alive: {t.is_alive()}")

update_and_post_data_to_db()