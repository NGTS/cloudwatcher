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

if __name__ == "__main__":
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
    n_meas = 5
    # loop forever
    while(1):
        with openPort() as port:
            outstr = ""
            # hand shake with central hub
            hub.report_in('cloud_watcher')
            for i in range(0, len(sen_name)):
                ngood = 0
                val_tot = []
                for j in range(0, n_meas):
                    z = sendRecv(port, sen_com[i], sen_buf[i])
                    if z is None:
                        break
                    if len(z) == sen_buf[i]:
                        ngood += 1
                        val = int(z.split('!')[spl[i]][1:])
                        val_tot.append(val)
                if z is None:
                    break
                # sigma clip
                val_av, clipped, med, std = clip(val_tot, ngood)
                # if a temeprature divide by 100
                if "Temp" in sen_name[i]:
                    val_av = temp(val_av)
                # correct the sky temp for the ambient temp
                if sen_name[i] == 'irSkyTemp':
                    val_av = corrSkyT(valstore['ambTemp'], val_av)
                # store the current values
                valstore[sen_name[i]] = val_av
                # print the output
                outstr = "{}[{}:{}] {:.2f}\t".format(outstr, ngood,
                                                     clipped, val_av)
            # grab the PWM value once per set
            z = sendRecv(port, "Q", 30)
            try:
                valstore['PWM'] = int(z.split('!')[1][1:])
            except AttributeError:
                valstore['PWM'] = 0
            outstr = "{}\t{}\t".format(outstr, valstore['PWM'])
            # grab the errors once per set
            z = sendRecv(port, "D", 75)
            e_list = z.split('!')
            errors['E1'] = int(e_list[1][2:])
            errors['E2'] = int(e_list[2][2:])
            errors['E3'] = int(e_list[3][2:])
            errors['E4'] = int(e_list[4][2:])
            # print the output
            outstr = "{}{}\t{}\t{}\t{}".format(outstr,
                                               errors['E1'],
                                               errors['E2'],
                                               errors['E3'],
                                               errors['E4'])
            t2 = Time(datetime.utcnow(), scale='utc')
            outstr = "{:.6f}\t{}".format(t2.jd, outstr)
            print(outstr)
            # log to the database
            bucket = (int(time.time())/60)*60
            tsample = datetime.utcnow().isoformat().replace('T', ' ')
            logResults(host, tsample, bucket, valstore, errors)
