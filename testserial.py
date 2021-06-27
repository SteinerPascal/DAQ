import serial
import serial.tools.list_ports
import logging 
import datetime
from dateutil import tz

# Script to quickly test and debug serial Connection
ser=serial.Serial('/dev/ttyACM0',115200)
tzone = tz.gettz("Europe/Zurich")
while(True):
    try:
        msg_line = ser.readline().rstrip().decode('UTF-8','ignore')
    except Exception as e:
        logging.error('ser.readline() exception caught')
        logging.error(f'{datetime.datetime.now(tz=tzone)}',exc_info=1)
    print(msg_line)