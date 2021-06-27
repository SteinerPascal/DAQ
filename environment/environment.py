#!/usr/bin/env python3
import threading
import serial
import argparse
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
import logging
import datetime
from dateutil import tz
from pathlib import Path
import queue
import serial.tools.list_ports

# thread safe
q = queue.Queue()

# format from Serial connection:
# 't/h/p,%.2f,%.1f,%.2f,l,%u,%u,%u,%u',
# t=temp/h=humidity/p=pressure/l=light
def writeSingleMeasurement(splitmsg,influxdb):
    msrs = []
    msrs.append(Point('temperature').field('value', float(splitmsg[2])).time(int(splitmsg[0]), write_precision='s'))
    msrs.append(Point('humidity').field('value', float(splitmsg[3])).time(int(splitmsg[0]), write_precision='s'))
    msrs.append(Point('pressure').field('value', float(splitmsg[4])).time(int(splitmsg[0]), write_precision='s'))
    msrs.append(Point('light.red').field('value', int(splitmsg[6])).time(int(splitmsg[0]), write_precision='s'))
    msrs.append(Point('light.green').field('value', int(splitmsg[7])).time(int(splitmsg[0]), write_precision='s'))
    msrs.append(Point('light.blue').field('value', int(splitmsg[8])).time(int(splitmsg[0]), write_precision='s'))
    msrs.append(Point('light.white').field('value', int(splitmsg[9])).time(int(splitmsg[0]), write_precision='s'))

    write_api.write(bucket=influxdb, record=msrs)
    

def writeMeasurements(msgs_out,influxdb):
  logging.info(f'writing messages')
  # parse and write measurements
  for msg in msgs_out:
      splitmsg = msg.split(',')
      writeSingleMeasurement(splitmsg,influxdb)
  
  logging.info(f'{datetime.datetime.now(tz=tzone)}; {len(msgs_out)} measurements written;')


# writerThread writes all the measurement to the InfluxDB every 1000 measure
def writerThread(influxdb):
  msgs_out = []
  while True:
    item = q.get()
    msgs_out.append(item)
    q.task_done()
    if len(msgs_out) >= 1000:
      try:
        writeMeasurements(msgs_out,influxdb)
        msgs_out = []
      except Exception:
        logging.error('write measurements error')
        logging.error(f'{datetime.datetime.now(tz=tzone)}',exc_info=1)
        pass


# readerThread reads continously the serial port       
def readerThread(port,speed):
  # keep this function simple
  logging.info('opening serial...')
  logging.info(f'argument: {port}; speed: {speed}')
  ser = serial.Serial(port, speed)
  while True:
    msg_line = ''
    
    try:
      msg_line = ser.readline().rstrip().decode('UTF-8')
      now = str(int(time.time()))
      msg_line = now + ',' + msg_line
      q.put(msg_line)
    except Exception:
      ser.close()
      time.sleep(2)
      logging.error('ser.readline() exception caught')
      logging.error(f'{datetime.datetime.now(tz=tzone)}',exc_info=1)
      logging.error('Probably Watchdog timer gots activated. Arduino is resetting')
      logging.error('restarting serial connection...')
      ser = serial.Serial(port, speed)
      time.sleep(1)
      continue

      
def main(args):
  reader = threading.Thread(target = readerThread, name = 'reader', args = (args.port,args.speed), daemon = True)
  writer = threading.Thread(target = writerThread, name = 'writer', args = (args.influxdb,), daemon = True)
  reader.start()
  writer.start()
  reader.join()
  writer.join()  


def startLogger():
  logfile = ''
  for i in range(0,20):
    logfile = Path(f'{i}_environment.log')
    if logfile.is_file():
        continue
    break
  logging.basicConfig(filename=str(logfile), level=logging.INFO ,filemode='w', format='%(name)s - %(levelname)s - %(message)s')

# starting point
if __name__ == '__main__':
  # Start loggin
  startLogger()
  # define timezone
  tzone = tz.gettz('Europe/Zurich')

  # Get port of arduino nano 33 BLE sense
  list = serial.tools.list_ports.grep('Nano 33 BLE', include_links=False)
  port = next(list, False)
  # parse all arguments provided
  parser = argparse.ArgumentParser(description='data relay and message queue')
  parser.add_argument('--port', type=str, default=port.device, help='serial port connected to Arduino')
  parser.add_argument('--speed', type=int, default=9600, help='serial port speed')
  parser.add_argument('--influxserver', type=str, default='http://localhost', help='InfluxDB server host / IP')
  parser.add_argument('--influxport', type=int, default=8086, help='InfluxDB port')
  parser.add_argument('--influxdb', type=str, default='env_01_06', help='InfluxDB db name')
  args = parser.parse_args()

   # validate port
  if port is False:
    logging.info(f'{datetime.datetime.now(tz=tzone)}: Port not autamitaccally found and none porvided in args')

 
  client = InfluxDBClient(url='http://'+args.influxserver + ':' + str(args.influxport), token='yourtoken', org='ignored')
  write_api = client.write_api(write_options=ASYNCHRONOUS)
  main(args)
 
    