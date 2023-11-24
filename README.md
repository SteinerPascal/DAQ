# DAQ for plant Electrophysiology
Simple Data Aquisition (DAQ) on Raspberry Pi for plant electrophysiology project written in python. This project was part of a bigger setup for a semester assignment. Be aware: This is an experimental project and only a prototype!

## Project setup
This setup consisted of two parts. The first is for measuring plant signals and the second for the environmental data.

## Plant side
This setup used an ESP32 Devkit1 connected to an 16Bit ADC1115. The ADC1115 does differential measurements on a single plant. It uses electrodes made of copper with a silver mantle. Tests have been done with accupuncture needles made from stainless steel. Make sure your electrodes are within 0.4mm in diameter.

Code can be found [here](https://github.com/SteinerPascal/plant-logger)

### ADC1x15
An ADC with a lower resolution such as the 12Bit ADC1110 could also be enough but you would need to go for a gain of at least 8x. 12Bit on a gain of 8x means that the LSB is 0.25mV. The signal expected electrophysiological changes are in a range of 10-50mV. So 0.25mV as the smallest recognizable change should be enough. Be aware though: In my tests i measured a drop in voltage the higher i set the gain. A gain of 8 already halfed the signal. That's why I chose a 16Bit ADC (16Bit,gainx1 means LSB is 0.125mV).

### ESP32
The ESP32 was connected over the USB UART connection to a Raspberry Pi. The Low Pass filter is set on 10Hz since all the interesting signals are going to be below it. Note: The Filter can also be staged. the sampling time can be adjusted in the Adafruit ADC Library [here](https://github.com/adafruit/Adafruit_ADS1X15/blob/master/Adafruit_ADS1X15.cpp#L52) but keep the [nyquist rate](https://en.wikipedia.org/wiki/Nyquist_rate) in mind.

To test the digital filter you can use a script for scilab. Then just connect an aux cable to your ADC and check the filtered results. Script is copied from [this](https://www.allaboutcircuits.com/technical-articles/how-to-use-your-computer-as-an-arbitrary-waveform-generator/) blogpost.

## Environment side
For environmental data logging a Arduino Nano 33 Sense was used. It implements a watchdog function to keep the recording alive in case anything goes wrong. Currently it logs temperature, hummidity, light and pressure. It was connected via USB UART to a Raspberry Pi.

Code can be found [here](https://github.com/SteinerPascal/environment-logger)

## Raspberry Pi
Two python scripts were running on the RPi. Both of them implement a simple producer-consumer pattern with a thread safe queue. They send the data to an InfluxDB 1.8 (InfluxDB 2.0 requires a 64xbit system which my RPi does not support). On the InfluxDB1.8 data can be visualized with grafana or chronograf. 
To start the script: ```nohup python3 -u [scriptname]```


## Some useful Stuff
- check for USB ports: `ps -ef | grep tty`
- check for USB ports with python serial lib: `python3 -m serial.tools.miniterm`
- check which pythonscripts are running: `ps -aef | grep python`
- Continous query on Influx for data aggregation: `CREATE CONTINUOUS QUERY cq_mv_avg ON plant_raw_01_06 RESAMPLE EVERY 1h FOR 1h BEGIN SELECT mean(value) INTO plant_raw_01_06.forever.avg_mv_value FROM plant_raw_01_06.oneday.mv GROUP BY time(10s) END`
