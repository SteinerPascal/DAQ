#!/usr/bin/env python3
from influxdb_client.domain.write_precision import WritePrecision
import serial
from influxdb_client import InfluxDBClient, Point
import argparse
import threading
from influxdb_client.client.write_api import ASYNCHRONOUS
import time
import queue
import logging
import datetime
from dateutil import tz
from pathlib import Path

# Thread-safe queue
q = queue.Queue()
write_out = []
write_in = []

def writeMeasurements(write_out):
  try:
    write_api.write(bucket="plant_raw_01_06", record=write_out)
    write_out.clear()
  except Exception as e:
    logging.error(f'{datetime.datetime.now(tz=tzone)};',exc_info=1)
    
# writerThread gets measurements from queue and writes it to database        
def writerThread():
    while True:       
        item = q.get()
        write_out.append(item)
        q.task_done()
        if(len(write_out) > 5500):
          writeMeasurements(write_out)
          logging.info(f'{datetime.datetime.now(tz=tzone)}; measurements written')

# readerThread reads continously measurements and stores it to queue
def readerThread(port, speed):
  logging.info(f'starting serial with: {port}')
  logging.info(f'port: {port} and speed: {speed}')
  ser = serial.Serial(port, speed)
  while True:
    try:
      msg = ser.readline().rstrip().decode('utf-8','ignore')
      splitmsg = msg.split(",")
      # timestamp to aggregate
      now = int(round(time.time() * 1000))
      if(len(splitmsg)==4 and splitmsg[0] == "mv" and splitmsg[2] == "filtered" and isfloat(splitmsg[3]) and isfloat(splitmsg[1]) ):
        # Format from Serial: 
        # "mv,%.3f,filtered,%.3f"
        q.put(Point(splitmsg[2]).field("value", float(splitmsg[3])).time(now, write_precision="ms"))
        q.put(Point(splitmsg[0]).field("value", float(splitmsg[1])).time(now, write_precision="ms"))
    except Exception:
      # Arduino was probably reset, try to connect again
      ser.close()
      time.sleep(2)
      logging.error('ser.readline() exception caught')
      logging.error(f'{datetime.datetime.now(tz=tzone)}',exc_info=1)
      logging.error('restarting serial connection...')   
      ser = serial.Serial(port, speed)
      time.sleep(2)
      continue


def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def startLogger():
  logfile = ""
  for i in range(0,20):
    logfile = Path(f'{i}_plant.log')
    if logfile.is_file():
        continue
    break
  logging.basicConfig(filename=str(logfile), level=logging.INFO ,filemode='w', format='%(name)s - %(levelname)s - %(message)s')

def main(args):
  reader = threading.Thread(target = readerThread, name = 'readerThread', args = (args.port,args.speed), daemon = True)
  reader.start()

  writer = threading.Thread(target = writerThread, name = 'writerThread', daemon = True)
  writer.start()

  reader.join()
  writer.join()

# starting point
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="data relay and message queue")
  # /dev/ttyACM0 = right upper corner
  parser.add_argument('--port', type=str, default='/dev/ttyACM0', help='serial port connected to Arduino')
  parser.add_argument('--speed', type=int, default=115200, help='serial port speed')
  parser.add_argument('--influxserver', type=str, default='http://localhost', help='InfluxDB server host / IP')
  parser.add_argument('--influxport', type=int, default=8086, help='InfluxDB port')
  parser.add_argument('--influxdb', type=str, default='plant_raw', help='InfluxDB db name')
  args = parser.parse_args()

  ## Start loggin
  startLogger()

  tzone = tz.gettz("Europe/Zurich")

  client = InfluxDBClient(url=args.influxserver + ":" + str(args.influxport), token="your token", org="ignored")
  write_api = client.write_api(write_options=ASYNCHRONOUS)
  
  main(args)