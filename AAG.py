#!/usr/local/python/bin/python
###############################################################################
#                                                                             #
#               Script to poll the AAG CloudWatcher weather sensor            #
#                                    v1.3                                     #
#                               James McCormac                                #
#                                                                             #
# Version History:                                                            #
#	20150705	v1.0	Code written and tested                               #
#	20150713	v1.1	Tested with TCP/IP connection                         #
#	20150715	v1.2	Added database logging, more sensors and tidy up      #
#	20151130	v1.3	Added central hub handshake with Pyro                 #
#                                                                             #
###############################################################################
#
# Temperatures of IR sensor and IR measurement are converted to degC 
#		by dividing by 100.
#
# A! - Device name
# B! - Firmware version
# K! - serial number
# T! - Ambient temperature (/100 to get value)
# S! - IR sky temperature (/100 to get value)
# E! - Rain frequency (2560=dry, <2560=wet, single drop = 2300)
# C! - LDR voltage + Rain sensor temp (!4=1022 = dark, !4=11 = bright - around sunset starts to increase, seen from home)
# D! - Device errors
#
# To do: 
#	add PWM control
#	use re to pull out info from strings
#

import time, math, sys
import numpy as np
from astropy.time import Time
from datetime import datetime 
import argparse as ap
import getpass, signal, pymysql, socket
import Pyro4

# start up Pyro connection
hub=Pyro4.Proxy("PYRONAME:central.hub")

# parse command line
def ArgParse():
	parser=ap.ArgumentParser()
	parser.add_argument('conn',help="tcpip | rs232",choices=['tcpip','rs232'])
	parser.add_argument('log',help="db | text",choices=['db','text'])
	args=parser.parse_args()
	return args

# function to send & receive from 
# tcpip or 232 port
def SendRecv(ctype,val,buff_size):
	if ctype == "rs232":
		s.write("%s!" % (val))
		z=s.read(size=buff_size)
	else:
		s.send("%s!" % (val))
		time.sleep(1)
		z=s.recv(buff_size)
	return z

# correct the IR temps
def Temp(x):
	return x / 100.

# correct the sky temp for the ambient
def corrSkyT(ambT,skyT):
	# sky temp correction terms
	k=[33,0,4,100,100]
	Tc =((k[0]/100.)*(ambT -k[1]/10.))+(k[2]/100.)*pow((np.exp(k[3]/1000.*ambT)),(k[4]/100.))
	return skyT-Tc	
	
# sigma clip the n_meas measurements before 
# publishing them - advised by AAG
def clip(val_tot,ngood):
	med=np.median(val_tot)
	std=np.std(val_tot)
	
	val_tot_c=0
	clipped=0
	if std != 0.0:
		for k in val_tot:
			if k > med-std and k < med+std:
				val_tot_c+=k
			else:
				clipped +=1	
		val_av=float(val_tot_c)/(ngood-clipped)
	else:
		val_av=float(np.sum(val_tot))/ngood
	
	return val_av, clipped, med, std

# set up Ctrl+C handling
die=False
def signal_handler(signal,frame):
	global die
	print "Ctrl+C caught, exiting..."
	die=True
	
signal.signal(signal.SIGINT,signal_handler)
	
# parse the command line etc.
args=ArgParse()
me = getpass.getuser()
host=socket.gethostname()

# set up the connection
if args.conn=="rs232":
	import serial
	s=serial.Serial('/dev/tty.usbserial-FTHM42UH',9600,bytesize=8,parity='N',stopbits=1,timeout=1)
	op=s.isOpen()
	if op != True:
		s.open()
else:
	TCP_IP='10.2.5.93'
	TCP_PORT=4004
	s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	s.connect((TCP_IP,TCP_PORT))
	s.settimeout(1)
	s.setblocking(False)

# set up the sensors
sen_name=['ambTemp','rainFreq','irSkyTemp','LDR','rainSensTemp']
sen_com=['T','E','S','C','C']
spl=[1,1,1,2,3]
sen_buf=[30,30,30,60,60]
valstore={'ambTemp':0,'rainFreq':0,'irSkyTemp':0,'LDR':0,'rainSensTemp':0,'PWM':0}
errors={'E1':0,'E2':0,'E3':0,'E4':0}
n_meas=5

# text file or database?
if args.log=='text':
	if me == 'James':
		outfile="/Users/James/Desktop/AAG_test.log"
	elif me == 'ops':
		outfile='/home/ops/jmcc/AAG_test.log'
	else:
		print "Whoami!?"
		sys.exit(1)
		
	f=open(outfile,'a')
	f.write('#JD\t[g:c] ambTemp\t[g:c] rain\t[g:c] skyTemp\t[g:c] LDR\t[g:c] rainSensTemp\tPWM\tE1\tE2\tE3\tE4\n')
	f.close()
else:
	if me=="ops":
		db=pymysql.connect(host='ds',db='ngts_ops')
		
# loop forever
while(1):
	outstr=""
	# hand shake with central hub
	hub.report_in('cloud_watcher')	
	for i in range(0,len(sen_name)):
		ngood=0
		val_tot=[]
		for j in range(0,n_meas):
			z=SendRecv(args.conn,sen_com[i],sen_buf[i])
			if len(z) == sen_buf[i]:
				ngood+=1
				val=int(z.split('!')[spl[i]][1:])
				val_tot.append(val)
		
		# sigma clip
		val_av,clipped,med,std=clip(val_tot,ngood)
		
		# if a temeprature divide by 100
		if "Temp" in sen_name[i]:
			val_av=Temp(val_av)
		
		# correct the sky temp for the ambient temp
		if sen_name[i]=='irSkyTemp':
			val_av=corrSkyT(valstore['ambTemp'],val_av)
		
		# store the current values
		valstore[sen_name[i]]=val_av
		
		# print the output
		outstr="%s[%d:%d] %.2f\t" % (outstr,ngood,clipped, val_av)	
	
	# grab the PWM value once per set
	z=SendRecv(args.conn,"Q",30)
	valstore['PWM'] = int(z.split('!')[1][1:])
	outstr="%s\t%d\t" % (outstr,valstore['PWM'])
	
	# grab the errors once per set
	z=SendRecv(args.conn,"D",75)		
	e_list=z.split('!')
	errors['E1'] = int(e_list[1][2:])
	errors['E2'] = int(e_list[2][2:])
	errors['E3'] = int(e_list[3][2:])
	errors['E4'] = int(e_list[4][2:])
		
	# print the output
	outstr="%s%d\t%d\t%d\t%d" % (outstr,errors['E1'],errors['E2'],errors['E3'],errors['E4'])		
	
	if args.log == 'text':
		t=datetime.utcnow()
		t2=Time(t,scale='utc')
		outstr="%.6f\t%s" % (t2.jd, outstr)
		print outstr
		f=open(outfile,'a')
		f.write(outstr+"\n")
		f.close()
	else:
		if me=='ops': 
			bucket=(int(time.time())/60)*60
			tsample=datetime.utcnow().isoformat().replace('T',' ')
			qry='REPLACE INTO cloudwatcher (tsample,bucket,ambient_temp,rain_freq,sky_temp_c,ldr,rain_sens_temp,pwm,e1,e2,e3,e4,host) VALUES ("%s",%d,%.2f,%d,%.2f,%d,%.2f,%d,%d,%d,%d,%d,"%s")' % (tsample,bucket,valstore['ambTemp'],valstore['rainFreq'],valstore['irSkyTemp'],valstore['LDR'],valstore['rainSensTemp'],valstore['PWM'],errors['E1'],errors['E2'],errors['E3'],errors['E4'],host)
			with db.cursor() as cur:
				cur.execute(qry)	
				db.commit()
			print outstr
	
	# close up 		
	if die == True:
		s.close()
		print "Socket closed"
		if args.log == 'db':
			db.close()
			print "Database disconnected"
		sys.exit(1)

