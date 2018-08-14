#!/usr/bin/env python
# -*- coding: cp949 -*-

import os
import time, threading
import datetime
import MySQLdb
import pymssql
from subprocess import *

ADDR = 'Address'
RSSI = 'RSSI'
NAME = 'Name'

parsor_text = ['Event type:', 'Address:', 'Name', 'RSSI:', 'TX power:', 'Service Data (UUID 0x180f):', 'Service Data (UUID 0x1809):']
j = 0

MAT = ' '

#============ MySQL Init ==============
#global gGatewayIndex, gGatewayName, gGatewayIP, gSqlIndex, gBLEMAC, gBLELocation_info, gBLERSSI,gLoc_detail_2,gLoc_detail_3,gBLETime
gGatewayIndex = 1
gGatewayName = 'AnyBi:n_001'
gGatewayIP = '127.0.0.1'
gSqlIndex = 0
gBLEMAC = ' '
gBLELocation_info = 'A Zone'
gBLERSSI = ' '
gLoc_detail_2 = ' '
gLoc_detail_3 = ' '
gBLETime = ' '
gBLENAME = ' '
gIPADDR = '127.0.0.1'
gTXPower = ' '
gBatteryLevel = ' '
gTemperature = ' '
#======================================
gBLEMAC_L = ' '
gBLERSSI_L = ' '
gBLENAME_L = ' '
gTXPower_L = ' '
gBatteryLevel_L = ' '
gTemperature_L = ' '

gIPAddress = ' '
gSCANNER_ID = ' '
gConstrain = ' '
gProductco = ' '
gLocation = ' '
gLocx = ' '
gLocy = ' '
gLocz = ' '
gDatetime = ' '

#======================================

def Get_IPADDRESS() :
	global gGatewayIP
	cmd = "ifconfig eth | grep 'inet addr:' | cut -d: -f2 | awk '{print $1}'"
	p = Popen(cmd, shell=True, stdout=PIPE)
	output = p.communicate()[0]
	gGatewayIP = output
	if(output != "") : 
		print("My IP Address : %s") % (output)
		return(output)
	else :
		cmd = "ifconfig wlan | grep 'inet addr:' | cut -d: -f2 | awk '{print $1}'"
		p = Popen(cmd, shell=True, stdout=PIPE)
		output = p.communicate()[0]
		gGatewayIP = output
		print("My IP Address : %s") % (output)
		return(output)

#======================================

def SQL_OPEN() :
	global db, curs

	db = MySQLdb.connect("localhost", "bleuser", "1234", "mlms_db")
	curs = db.cursor()

#======================================

def SQL_SHOW_TABLE() :
	curs.execute("""show tables""")
	for r in curs.fetchall() :
		print(r)

#======================================

def SQL_AddGatewayInfo() :
	#global gGatewayName, gGatewayIP
	global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime

	gDatetime = datetime.datetime.now()

	query = "INSERT INTO ble_scanner (bs_name, bs_ipaddress, bs_constraincondition, bs_productco, bs_managelocation, bs_locX, bs_locY, bs_locZ, bs_datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
	#curs.execute(query, (gGatewayName,  gGatewayIP))
	curs.execute(query, (gSCANNER_ID,  gIPAddress, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime))

	for r in curs.fetchall() :
		print(r)

#======================================

def SQL_UpdateGatewayInfo() : 
	global gGatewayName, gGatewayIP
        global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime

	query = "UPDATE ble_scanner SET bs_name=%s, bs_constraincondition=%s, bs_productco=%s, bs_managelocation=%s, bs_locX=%s, bs_locY=%s, bs_locZ=%s, bs_datetime=%s WHERE bs_ipaddress=%s"
	curs.execute(query, (gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime, gIPAddress))
	for r in curs.fetchall() :
                print(r)

#======================================

def SQL_getCountGateway() :
        global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime
	query = "select count(bs_name) from ble_scanner where bs_ipaddress = %s"
	curs.execute(query, gIPAddress)
	for r in curs.fetchall() : 
		print(r)
	a = r[0]
	return(int(a))

#======================================

def SQL_getGatewayIndex() : 
	global gGatewayName, gGatewayIP
	global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime

	query = "select bs_Index from ble_scanner where bs_name = %s and bs_ipaddress = %s"
        curs.execute(query, (gSCANNER_ID, gIPAddress))
        for r in curs.fetchall() :
                print(r)
        a = r[0]
        return(int(a))

#======================================

def SQL_BLEGATEWAY_INIT() :
	global gSqlIndex
	ret = SQL_getCountGateway()
	if(ret == 0) :
    		print("no data"),
		SQL_AddGatewayInfo()
		db.commit()
		print("Data Committed")

	else :
        	print("Update:"),    #,without carrige return
		SQL_UpdateGatewayInfo()
		db.commit()	
		print("Data Committed")

		gSqlIndex = SQL_getGatewayIndex();
		print("Gateway Index = %d") % (gSqlIndex)

#======================================

def SQL_InsertBLE() :
	global gSCANNER_ID, gBLENAME, gBLEMAC, gBLERSSI, gBatteryLevel, gBLETime, gTemperature, gTXPower

	#print('************************************SQL_InsertBLE()******************************')

	query = "INSERT INTO ble_device (blackbin_bs_name, blackbin_name, blackbin_macaddress, blackbin_rssi, blackbin_batterylevel, blackbin_receivedatetime) VALUES(%s, %s, %s, %s, %s, %s)"

	curs.execute(query, (gSCANNER_ID, gBLENAME, gBLEMAC, gBLERSSI, gBatteryLevel, gBLETime))

	for r in curs.fetchall() :
                print(r)

	db.commit()

#======================================

def SQL_UpdateBLE(s) :

	#global gGatewayIndex, gGatewayName, gGatewayIP, gSqlIndex, gBLEMAC, gBLELocation_info, gBLERSSI,gLoc_detail_2,gLoc_detail_3,gBLETime
    	#global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime
	global gSCANNER_ID, gBLENAME, gBLEMAC, gBLERSSI, gBatteryLevel, gTemperature, gBLETime, gTXPower

	query = "select count(*) from ble_scanner where BLEID = %s"
	curs.execute(query, s)
	for r in curs.fetchall() :
		print(r)
		qcnt = int(r[0])
		print("BLE count = %d") % (qcnt)

	if(qcnt > 0) :
		print("BLE Update")
		print("gSqlIndex : %d") % (gSqlIndex)
		query = "UPDATE ble_device SET Loc_detail_1=%s, Receive_Time=%s WHERE BLEID=%s"
                curs.execute(query, (gBLERSSI, gBLETime, s))
                #print (query)
                db.commit()

	else :
		print("BLE Insert : %s") % (s)
		print("gSqlIndex : %d") % (gSqlIndex)
		query = "INSERT INTO ble_device (GatewayIndex, BLEID, Location_info, Loc_detail_1, Loc_detail_2, \
			Loc_detail_3, Receive_Time) VALUES (%s, %s, %s, %s, %s, %s, %s)"
		curs.execute(query, (int(gSqlIndex), s, gBLELocation_info, gBLERSSI, gLoc_detail_2, gLoc_detail_3, gBLETime))
		#print (query)
		db.commit()

#======================================

def val_text(s, isPrint) : 
	global MAT
	global gBLEMAC, gBLERSSI, gBLETime, gBLENAME
	
	if(s == ':') : 
		MAT = s + ' '
	else :
		tt = MAT
		MAT = tt + s + ' '

	if(isPrint == True) :
		s = datetime.datetime.now()
		gBLETime = s

		if(gBLEMAC != ' ') :
			SQL_UpdateBLE(gBLEMAC)
		
		#MAT = MAT + s
		print ('---------------------------------------------')
		print (MAT),
		print (s)
		print ('=============================================')
		MAT = ' '

#======================================

def find_string(s) :
	global gBLEMAC, gBLERSSI, gBLETime, gBLENAME, gTXPower, gBatteryLevel, gTemperature
	global gBLEMAC_L, gBLERSSI_L,  gBLENAME_L, gTXPower_L, gBatteryLevel_L, gTemperature_L

	start_flag = 2

	for i in parsor_text :
		#range(k, parsor_text)
		u = s.find(i)
		if(u > -1) :
			# Data find
			l = s.split()

			if(i == parsor_text[0]) :
				#Event type : ADV_IND & SCAN_RSP
				mat = l[5]
				#print('==============Start===========')
				ret = cmp('ADV_IND', mat)
				if(ret == 0) :
					#print('ADV_IND', mat)
					start_flag = 0
				
				else :
					#print('SCAN_RSP', mat)
					start_flag = 1

			elif(i == parsor_text[1]) :
				#MAC Address
				mat = l[1]
				gBLEMAC_L = mat
				#print('MAC ADDRESS : %s') % (gBLEMAC_L)
				#val_text(mat, False)
				#print(l)
				#print(mat)
				#j = j+1
			elif(i == parsor_text[2]) :
				#BLE NAME

				name_len = len(l)
				
				mat = l[2]
				
				for h in range(name_len-3) :
					mat += l[h+3]

				gBLENAME_L = mat
				#print('BLE NAME : %s') % (gBLENAME_L)
				#val_text(mat, False)
				#print(l)
				#print(mat)
				#j = j+1
			elif(i == parsor_text[3]) :
				#BLE RSSI
				mat = l[1]
				gBLERSSI_L = mat
				#print('RSSI : %s') % (gBLERSSI_L)
				#val_text(mat, True)
				#print(l)
				#print(mat)
				#j = j+1
			elif(i == parsor_text[4]) :
				#TX Power
				mat = l[2]
				gTXPower_L = mat
				#print('TX Power : %s') % (gTXPower_L)

			elif(i == parsor_text[5]) :
				#battery level
				mat = l[4]
				gBatteryLevel_L = mat
				#gBatteryLevel_L = int(mat, 16)
				#print(l)
				#print('Battery Level : %s %%') % (gBatteryLevel_L)

			elif(i == parsor_text[6]) :
				#temperature
				mat = l[4]
				gTemperature_L = mat
				#gTemperature_L = int(mat, 16)
				#print(l)
				#print('Temperature : %s degree') % (gTemperature_L)
			
			#print(mat[j])
			#print(l)
			#print(mat)
			#j = j+1
	#j = j+1
	
	#=============== print =================
	if(start_flag == 0) :
		s = datetime.datetime.now()
		gBLETime = s
     	        
        	gBLEMAC = gBLEMAC_L 
		gBLERSSI = gBLERSSI_L
		gBLENAME = gBLENAME_L
		gTXPower = gTXPower_L
		gBatteryLevel = gBatteryLevel_L
		gTemperature = gTemperature_L

		BLE_PRINT()
		SQL_InsertBLE()		#ble_device insert
		

        	gBLEMAC_L = ' '
		gBLERSSI_L = ' '
		gBLENAME_L = ' '
		gTXPower_L = ' '
		gBatteryLevel_L = ' '
		gTemperature_L = ' '

#======================================

def BLE_PRINT() :
	global gBLEMAC, gBLERSSI, gBLETime, gBLENAME, gTXPower, gBatteryLevel, gTemperature

	if(gBLEMAC != ' ') :
		print('--------------------------------------------------------------------------')
		print('%s, %s, %s, %s, %s, %s : %s') % (gBLEMAC, gBLERSSI, gBLENAME, gTXPower, gBatteryLevel, gTemperature, gBLETime)
	    	print('==========================================================================')

#======================================
	
def BTMON_START() :
	p = Popen(['btmon'], shell=True, stdout=PIPE)
	#p1 = Popen(['hcitool lescan'], shell=True, stdout=PIPE)
	while True :
        	#Len = len(p.stdout.readline())
	        #test = p1.stdout.readline()
		p.poll()
        	data = p.stdout.readline()
	        #print('================================================')
		if(data) :
			#print('H:')
			find_string(data)
		    	#print(data)
			#retOutputList.append(data)
			#if(bLoggingOutput) :
			#	self.Log(data)
		#if(data == ""):
			#print('000000000000000000000000000')
			#BLE_PRINT()
			# BLE insert 
			#break;

#======================================

def Read_File() :
        global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime
        parsor_t = ['ipaddress:', 'scannerid:', 'constrain:', 'productco:', 'lcation:', 'locx:', 'locy:', 'locz:']

        try :
                f = open('/home/pi/Anybin/HBLE/anybin_env', 'rb')

		gDatetime = datetime.datetime.now()

                for i in parsor_t :
                        line = f.readline()
                        l = line.split()

                        if(i == parsor_t[0]) :
                                # ipaddress
                                mat = l[1]
                                gIPAddress = mat
                                print(mat)
                        elif(i == parsor_t[1]) :
                                # scannerid
                                mat = l[1]
                                gSCANNER_ID = mat
                                print(mat)
                        elif(i == parsor_t[2]) :
                                # constrain
                                mat = l[1]
                                gConstrain = mat
                                print(mat)
                        elif(i == parsor_t[3]) :
                                # productco
                                mat = l[1]
                                gProductco = mat
                                print(mat)
                        elif(i == parsor_t[4]) :
                                # location
                                mat = l[1]
                                gLocation = mat
                                print(mat)
                        elif(i == parsor_t[5]) :
                                # locx
                                mat = l[1]
                                gLocx = mat
                                print(mat)
                        elif(i == parsor_t[6]) :
                                # locy
                                mat = l[1]
                                gLocy = mat
                                print(mat)
                        elif(i == parsor_t[7]) :
                                # locz
                                mat = l[1]
                                gLocz = mat
                                print(mat)

        except IndexError as e:
                print(e)

        finally :
                f.close()

#======================================

def main() : 
	Get_IPADDRESS()
	SQL_OPEN()
    	SQL_SHOW_TABLE()
	
	#SQL_UpdateBLE("11:00:22:00:33:00")

	# variable init
	print("==========================================================")
	print("       [ Red bi:n ] Ver 1.0 - BLE Scanner                 ")
	print("==========================================================")

	#val_text(':', True)
	
	Read_File()

    	SQL_BLEGATEWAY_INIT()

	BTMON_START()

#======================================

if __name__ == '__main__' :
	main()

