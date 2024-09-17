#!/usr/local/python/bin/python
"""
A! - Device name
B! - Firmware version
K! - serial number
T! - Ambient temperature (/100 to get value)
S! - IR sky temperature (/100 to get value)
E! - Rain frequency (2560=dry, <2560=wet, single drop = 2300)
C! - LDR voltage + Rain sensor temp (!4=1022 = dark,
                                     !4=11 = bright)
D! - Device errors
"""
import time
from contextlib import contextmanager
import socket
from datetime import datetime
from astropy.time import Time
import numpy as np
import pymysql
import Pyro4
import argparse

# pylint: disable=invalid-name
# pylint: disable=superfluous-parens
# pylint: disable=bare-except
# pylint: disable=global-statement
# pylint: disable=line-too-long
# pylint: disable=redefined-outer-name

# start up Pyro connection
hub = Pyro4.Proxy("PYRONAME:central.hub")

def sendRecv(port, val, buff_size):
    """
    function to send & receive from
    tcpip port
    """
    try:
        port.send("{}!".format(val))
        time.sleep(1)
        z = port.recv(buff_size)
    except socket.error:
        z = None
    return z

def temp(x):
    """
    correct the IR temps
    """
    return x / 100.

def corrSkyT(ambT, skyT):
    """
    correct the sky temp for the ambient
    """
    # sky temp correction terms
    k = [33, 0, 4, 100, 100]
    Tc = ((k[0]/100.)*(ambT -k[1]/10.))+(k[2]/100.)*pow((np.exp(k[3]/1000.*ambT)), (k[4]/100.))
    return skyT-Tc

def clip(val_tot, ngood):
    """
    sigma clip the n_meas measurements before
    publishing them - advised by AAG
    """
    med = np.median(val_tot)
    std = np.std(val_tot)
    val_tot_c = 0
    clipped = 0
    if std != 0.0:
        for k in val_tot:
            if k > med-std and k < med+std:
                val_tot_c += k
            else:
                clipped += 1
        if ngood == clipped:
            val_av = float(np.sum(val_tot))/ngood
        else:
            val_av = float(val_tot_c)/(ngood-clipped)
    else:
        val_av = float(np.sum(val_tot))/ngood
    return val_av, clipped, med, std

@contextmanager
def openPort():
    """
    Context manager for a TCP IP port
    """
    TCP_IP = '10.2.5.93'
    TCP_PORT = 4004
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((TCP_IP, TCP_PORT))
        s.settimeout(1)
        s.setblocking(False)
        yield s
    except socket.error:
        print('Cannot open port')
    finally:
        s.close()

def logResults(host, tsample, bucket, valstore, errors):
    """
    Log the output to the database
    """
    qry = """
        REPLACE INTO cloudwatcher
        (tsample, bucket, ambient_temp, rain_freq,
        sky_temp_c, ldr, rain_sens_temp, pwm, e1,
        e2, e3, e4, host)
        VALUES
        ("{}", {}, {:.2f}, {}, {:.2f}, {}, {:.2f},
        {}, {}, {}, {}, {}, "{}")
        """.format(tsample, bucket, valstore['ambTemp'],
                   valstore['rainFreq'], valstore['irSkyTemp'],
                   valstore['LDR'], valstore['rainSensTemp'],
                   valstore['PWM'], errors['E1'], errors['E2'],
                   errors['E3'], errors['E4'], host)
    try:
        with pymysql.connect(host='ds', db='ngts_ops') as cur:
            cur.execute(qry)
    except:
        print('Database connection error, skipping...')


def main():
    host = socket.gethostname()
    # set up the sensors
    sen_name = ['ambTemp', 'rainFreq', 'irSkyTemp', 'LDR', 'rainSensTemp']
    sen_com = ['T', 'E', 'S', 'C', 'C']
    spl = [1, 1, 1, 2, 3]
    sen_buf = [30, 30, 30, 60, 60]
    valstore = {'ambTemp':0,
                'rainFreq':0,
                'irSkyTemp':0,
                'LDR':0,
                'rainSensTemp':0,
                'PWM':0}
    errors = {'E1':0, 'E2':0, 'E3':0, 'E4':0}
    
    # A number of measurements are requested from the device
    # Which are then combined to obtain a better estimate of the true value
    n_meas = 5 

    # Loop forever
    while(1):
        try:
            with openPort() as port:
                outstr = ""
                # hand shake with central hub
                hub.report_in('cloud_watcher')

                # Request values for each quantity
                for i in range(0, len(sen_name)):
                    ngood = 0
                    val_tot = []

                    # Request measurements for this quantity
                    for j in range(0, n_meas):
                        z = sendRecv(port, sen_com[i], sen_buf[i])
                        if z is None:
                            break

                        # Ensure returned buffer size matches its expected size
                        if len(z) == sen_buf[i]:
                            ngood += 1
                            val = int(z.split('!')[spl[i]][1:])
                            val_tot.append(val)
                    
                    if z is None:
                        break
                    
                    # Sigma clip to remove edge values
                    val_av, clipped, med, std = clip(val_tot, ngood)

                    # Adjust temperature
                    if "Temp" in sen_name[i]:
                        val_av = temp(val_av)
                    
                    # Correct the sky temp for the ambient temp
                    if sen_name[i] == 'irSkyTemp':
                        val_av = corrSkyT(valstore['ambTemp'], val_av)
                    
                    # Store the current values
                    valstore[sen_name[i]] = val_av
                    
                    # Print the output
                    outstr = "{}[{}:{}] {:.2f}\t".format(outstr, ngood,
                                                         clipped, val_av)
                # Grab the PWM value once per set
                z = sendRecv(port, "Q", 30)
                try:
                    valstore['PWM'] = int(z.split('!')[1][1:])
                except AttributeError:
                    valstore['PWM'] = 0
                outstr = "{}\t{}\t".format(outstr, valstore['PWM'])
                
                # Grab the errors once per set
                z = sendRecv(port, "D", 75)
                try:
                    e_list = z.split('!')
                    errors['E1'] = int(e_list[1][2:])
                    errors['E2'] = int(e_list[2][2:])
                    errors['E3'] = int(e_list[3][2:])
                    errors['E4'] = int(e_list[4][2:])
                except AttributeError:
                    errors['E1'] = 0
                    errors['E2'] = 0
                    errors['E3'] = 0
                    errors['E4'] = 0
                # print the output
                outstr = "{}{}\t{}\t{}\t{}".format(outstr,
                                                   errors['E1'],
                                                   errors['E2'],
                                                   errors['E3'],
                                                   errors['E4'])
                t2 = Time(datetime.utcnow(), scale='utc')
                outstr = "{:.6f}\t{}".format(t2.jd, outstr)
                print(outstr)
                
                # Log to the database
                bucket = (int(time.time())/60)*60
                tsample = datetime.utcnow().isoformat().replace('T', ' ')
                logResults(host, tsample, bucket, valstore, errors)
                time.sleep(60)
        except RuntimeError:
            time.sleep(10)
            continue



##########################################
##########################################

# IP address of Moxa where cloudwatcher is connected
TCP_IP = '10.2.5.93'
TCP_PORT = 4004

# Number of seconds to wait for TCP response
TCP_AWAIT_SECONDS = 1

# Minimum number of measurements to take from each sensor
MIN_SAMPLES = 5



# Valid commands for the cloudwatcher.
#   bufsize: expected length in bytes of the response
#   nblocks: number of blocks in the response (each block is separated by a '!' character)
COMMAND_DATA = {
    'A' : {'bufsize':30 }, # Internal name
    'B' : {'bufsize':30 }, # Firmware version
    'C' : {'bufsize':60 }, # Sensor values
    'D' : {'bufsize':75 }, # Internal errors
    'E' : {'bufsize':30 }, # Rain frequency
    'F' : {'bufsize':30 }, # Switch status
    'Q' : {'bufsize':30 }, # Get PWM value
    'S' : {'bufsize':30 }, # Get sky IR temperature
    'T' : {'bufsize':30 }, # Get sensor temperature
    'K' : {'bufsize':30 }, # Serial number
}

# Info to fetch sensor data
#   cmd: command to send to device
#   idx: index (block) in the returned sensor data where measurement is located
SENSOR_DATA = {
    'ambient_temp'   : {'cmd':'T', 'block':1},
    'rain_freq'      : {'cmd':'E', 'block':1},
    'sky_temp_c'     : {'cmd':'S', 'block':1},
    'ldr'            : {'cmd':'C', 'block':2},
    'rain_sens_temp' : {'cmd':'C', 'block':3},
}

DEVICE_DATA = {
    'pwm'             : {'cmd':'Q', 'block':1},
    'device_name'     : {'cmd':'A', 'block':'N'},
    'firmware_version': {'cmd':'B', 'block':'V'},
    'serial_number'   : {'cmd':'K', 'block':1},
}

DEVICE_ERRORS = {'E1':0, 'E2':0, 'E3':0, 'E4':0}



class tcp_port:
    """
    Context manager for opening and closing TCP ports
    """
    
    def __init__(self, ip, port_num):
        self.ip = ip
        self.port_num = port_num
        
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.ip, self.port_num))
            self.socket.settimeout(1)
            self.socket.setblocking(False)
        
        except socket.error:
            print('[ERROR] Cannot open port at {}:{}'.format(self.ip, self.port_num))
            exit()

    def __enter__(self):
        return self
        
    def __exit__(self, dtype, value, traceback):
        self.socket.close()

    def send(self, cmd, verbose = False):
        """
        Sends command to device via TCP IP port, and returns the response.
        The returned response consist on a list of "blocks".
        """
        bufsize = COMMAND_DATA[cmd]['bufsize']

        if verbose:
            print("[INFO] TCP sending command: {}!".format(cmd))
        
        try:
            self.socket.send(cmd + '!')
            time.sleep(TCP_AWAIT_SECONDS) # Is this necessary?
            response = self.socket.recv(bufsize)

            if verbose:
                print("[INFO] TCP received response: {}".format(response))
            
            if len(response) != bufsize:
                print("[WARN] Incorrect number of bytes received (expected {}, got {})".format(bufsize, len(response)))
                return None

            # Extract blocks from message
            blocks = response.split('!')
            data = {}

            for block in blocks:
                key = block[:2].strip()
                value = block[2:].strip()

                # Ignore empty or handshaking block
                if key == '' or key == '\x11':
                    continue

                # Special case of requesting serial number
                if block[0] == 'K':
                    key = 'K'
                    value = block[1:].strip()

                data[key] = value
            
            return data
        
        except socket.error:
            print("[WARN] Failed to send TCP message")
            return None



def save_to_db(host, sensor_values, device_errors, pwm, debug = False):
    """
    Log the output to the cloudwatcher database
    """
    bucket = (int(time.time())/60)*60
    tsample = datetime.utcnow().isoformat().replace('T', ' ')

    qry = """
        REPLACE INTO cloudwatcher
        (tsample, bucket, ambient_temp, rain_freq,
        sky_temp_c, ldr, rain_sens_temp, pwm, e1,
        e2, e3, e4, host)
        VALUES
        ("{}", {}, {:.2f}, {}, {:.2f}, {}, {:.2f},
        {}, {}, {}, {}, {}, "{}")
        """.format(
        tsample,
        bucket,
        sensor_values['ambient_temp'],
        sensor_values['rain_freq'],
        sensor_values['sky_temp_c'],
        sensor_values['ldr'],
        sensor_values['rain_sens_temp'],
        pwm,
        device_errors['E1'],
        device_errors['E2'],
        device_errors['E3'],
        device_errors['E4'],
        host
    )

    if debug is True:
        print("[DEBUG] Query to save to database: ")
        print("[DEBUG] {}".format(qry))
        return

    try:
        with pymysql.connect(host='ds', db='ngts_ops') as cur:
            cur.execute(qry)
    except:
        print('[WARN] Database connection error, skipping...')


def get_input_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--nsamples', help="Number of measurements to take", type = int, default = 5)
    parser.add_argument('-v', '--verbose', help="Print extra information", action='store_true')
    parser.add_argument('--debug', help="Debug mode. No info is saved to the database", action='store_true')
    return parser.parse_args()


def print_device_info(port):
    device_name = port.send('A')
    firmware_version = port.send('B')
    serial_num = port.send('K')

    if device_name is None:
        print("[ERROR] Failed to connect to cloudwatcher!")

    print("[INFO] Connected to AAG Cloudwatcher")
    print("[INFO] Device name: {}".format(device_name))
    print("[INFO] Firmware version: {}".format(firmware_version))
    print("[INFO] Serial Number: {}".format(serial_num))


def fetch_measurements(port, sensor_data, nsamples):
    results = [ tcp_send(port, sensor_data['cmd'], sensor_data['bufsize']) for i in nsamples ]

def sigma_clip_samples(samples):
    pass

def fetch_device_errors(port):
    result = tcp_send(port, DEVICE_DATA['cmd'], DEVICE_DATA['bufsize'])
    errors = {"E"+str(i+1) : v[2:] for i,v in enumerate(result.split('!')) }
    return errors

def cloudwatcher():
    
    args = get_input_args()
    if args.nsamples <= MIN_SAMPLES:
        print("[ERROR] Number of samples must be greater than {}".format(MIN_SAMPLES))
    
    if args.debug:
        args.verbose = True

    # Initialise dict of sensor values
    sensor_values = {k:0 for k in SENSOR_DATA.keys()}

    host = socket.gethostname()
    if args.verbose:
        print("[INFO] Host: {}".format(host))

    hub = Pyro4.Proxy("PYRONAME:central.hub")
    if args.verbose:
        print("[INFO] Connected to central hub")

    with tcp_port(TCP_IP, TCP_PORT) as port:
            
        if args.verbose:
            print_device_info(port)

        for cmd in COMMAND_DATA:
            resp = port.send(cmd, verbose = args.verbose)
            print(resp)

        exit()

        while(1):

            # Calculate averaged sensor measurements
            for sensor_name, sensor_data in SENSOR_DATA.items():
                samples = fetch_measurements(port, sensor_data, args.nsamples)
                if samples is None:
                    print("[ERROR] Could not fetch measurements for sensor '{}'".format(sensor_name))
                    sensor_values[sensor_name] = 0 # Replace with error value, e.g. NaN
                    continue
                clipped_samples = sigma_clip_samples(samples)
                sensor_values[sensor_name] = np.mean(clipped_samples)
            
            # Apply specific adjustments to e.g. temperature
            
            # Fetch extra info, e.g. errors and pwm
            device_errors = fetch_device_errors()
            
            print(sensor_values)
            print(device_errors)

            # Save all to DB
            # save_to_db(host, sensor_values, device_errors, pwm, debug = False):


if __name__ == "__main__":
    # main()
    cloudwatcher()