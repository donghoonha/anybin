#!/usr/bin/env python
# -*- coding: cp949 -*-

#=============================================================================================
# BLE Beacon get Data Function
# Author : HADES
# Date : 2018. 01. 02
#=============================================================================================

from CreatedBTable import *

import binascii
import RPi.GPIO as GPIO
import pdb
import math
from collections import deque

j = 0

MAT = ' '

gLD_RSSI = 0

mac_filter = '53:4F:4C'

#start_flag = 2

parsor_ble_text = ['> HCI Event:','Event type:', 'Address:', 'TX power:', 'Service Data (UUID 0x180f):', 'Service Data (UUID 0x1809):', 'Company:', 'Data:', 'Name (complete):', 'RSSI:' ]
#parsor_text = ['> HCI Event:','Event type:', 'Address:', 'TX power:', 'Name (complete):','Service Data (UUID 0x180f):', 'Service Data (UUID 0x1809):', 'Company:', 'Data:', 'Name (complete):', 'RSSI:' ]
ble_event_type = ['ADV_IND','ADV_DIRECT_IND','ADV_NONCONN_IND','SCAN_RSP']
ble_data_flag = 0
ble_event_type_flag = 0

LD_MAT = []
queue = deque([])



#=====================================
gMydB = ' '
#======================================
def Calc_Distance(rssi) :

        try :
		''' 
                # d = 10^((tx_power - rssi)/(10*n))
                # n = 2 or 5 (variable
                # tx_power = 40 for 1m
                #tx_power = -34
                tx_power = -34
                rssi = int(RSSI, 10)
                #num = 3.2
                num = 4.2
                val = ( (tx_power - rssi) / (10 * num) )

                d = math.pow(10, val)

                #print('val = %f, rssi = %d, distance = %f')%(val, rssi, d)
		'''

                RSSI = int(rssi, 10)

		A = [-6.48098070e-07, -2.37306183e-04, -3.43871507e-02, -2.47958149e+00, -9.07174054e+01, -1.60103038e+03, -1.06824362e+04]
		d = A[0]*math.pow(RSSI, 6) + A[1]*math.pow(RSSI, 5) + A[2]*math.pow(RSSI, 4) + A[3]*math.pow(RSSI, 3) + A[4]*math.pow(RSSI, 2) + A[5]*math.pow(RSSI, 1) + A[6]

		#print d
                return(d/1000)
        except :
                print('Calc Distance Error')
#======================================
def BLE_PRINT() :
        try :
                #global gBLEMAC, gBLERSSI, gBLETime, gBLENAME, gTXPower, gBatteryLevel, gTemperature, gLDName
		global gLD_RSSI;

                if(gMydB.gBLEMAC != ' ') :
                        calc_distance = Calc_Distance(gMydB.gBLERSSI) #imsi distance test
                        print('----------------------------------------------------------------------------------------')
                        print('%s, %s, %s, %s, %s, %s : %s : d = %f m, %s, %s ') % (gMydB.gBLEMAC, gMydB.gBLERSSI, gMydB.gBLENAME, gMydB.gTXPower, gMydB.gBatteryLevel, gMydB.gTemperature, gMydB.gBLETime, calc_distance, gMydB.gLDName, gMydB.gLDRSSI)
                        print('========================================================================================')
        except :
                print('BLE_Print Error')

#======================================

def getBLEdata(i, l) :
        global ble_event_type_flag
        try :
                 if(i == parsor_ble_text[0]) :
                        # > HCI Event: get Time
                        del LD_MAT[:]

                        LD_MAT.append(l[10])

                        #pdb.set_trace()


                 elif(i == parsor_ble_text[1]) :
                        # Event Type: ADV_IND(0x00), ADV_DIRECT_IND(0x02), ADV_NONCONN_IND(0x03), SCAN_RSP(0x04)
                        #print(i, l[5], l[6])           #first start

                        if(l[5] == ble_event_type[0]) :
                                # ADV_INV(0x00)
                                #print(i, l[5])
                                #LD_MAT[1] = l[5]
                                ble_event_type_flag = 0

                                #temp = self.df.loc[self.parsor_ble_text[0]]
                                #self.df[0] = ([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])

                                #self.df.loc[self.parsor_ble_text[0]] = temp

                                LD_MAT.append(l[5])

                        elif(l[5] == ble_event_type[3]) :
                                # SCAN_RSP(0x04)
                                #print(i, l[5])
                                #LD_MAT[1] = l[5]
                                ble_event_type_flag = 4

                                LD_MAT.append(l[5])

                        if(l[6] == ble_event_type[2]) :
                                # ADV_NONCONN_IND(0x03)
                                #print(i, l[6])
                                #LD_MAT[1] = l[6]
                                ble_event_type_flag = 3

                                LD_MAT.append(l[6])

                 elif(i == parsor_ble_text[2]) :
                        #MAC Address
                        mat = l[1]
                        LD_MAT.append(mat)
                        gMydB.gBLEMAC_L = mat
                        #print('MAC ADDRESS : %s') % (gMydB.gBLEMAC_L)
                                        #val_text(mat, False)
                                        #print(l)
                                        #print(mat)
                                        #j = j+1
                 elif(i == parsor_ble_text[3]) :
                        #TX Power
                        mat = l[2]
                        LD_MAT.append(mat)
                        gMydB.gTXPower_L = mat
                                        #print('TX Power : %s') % (gMydB.gTXPower_L)

                 elif(i == parsor_ble_text[4]) :
                        #battery level
                        mat = l[4]
                        LD_MAT.append(mat)
                        gMydB.gBatteryLevel_L = mat
                                        #gBatteryLevel_L = int(mat, 16)
                                        #print(l)
                                        #print('Battery Level : %s , start_flag : %d') % (gMydB.gBatteryLevel_L, start_flag)

                 elif(i == parsor_ble_text[5]) :
                        #temperature
                        mat = l[4]
                        LD_MAT.append(mat)
                        gMydB.gTemperature_L = mat
                                        #gTemperature_L = int(mat, 16)
                                        #print(l)
                                        #print('Temperature : %s degree') % (gMydB.gTemperature_L)

                 elif(i == parsor_ble_text[6]) :
                        #Company
                        mat = l[3]
                        LD_MAT.append(mat)

                                        #print('Company:')
                                        #print(mat)
                                        #temp = hex(int(mat))
                                        #if(temp == 0x03E8) :
                                        #       print('LD')
                                        #       print(mat)
                                        #if(len(temp) == 6) :
                                                #print('Company:', mat)
                                                #stemp = temp[4:6] + temp[2:4]
                                                #mat = binascii.a2b_hex(stemp)
                                                #print (mat)
                                                #gLDName_L = mat

                 elif(i == parsor_ble_text[7]) :
                        #LD Data
                        mat = l[1]
                        LD_MAT.append(mat)
                        mat1 = binascii.a2b_hex(mat)
                        if( (len(mat1) >= 6) and (len(mat1) < 11) ) :
                                #print (mat1)
                                #print('data :',mat1)
                                #gLDName_L = gLDName_L + mat1
                                gMydB.gLDName_L = mat1[:-1]
                                gMydB.gLDRSSI_L = int(mat[-2:],16) - 256	#string end character
                                print(gMydB.gLDName_L, len(gMydB.gLDName_L))
                                print('LD RSSI : ', gMydB.gLDRSSI_L)
                                LD_MAT.append(gMydB.gLDName_L)
                                LD_MAT.append(gMydB.gLDRSSI_L)



                 elif(i == parsor_ble_text[8]) :
                        #BLE NAME

                        name_len = len(l)

                        mat = l[2]
                        LD_MAT.append(mat)
                        gMydB.gBLENAME_L = mat

                        #pdb.set_trace()

                        #for h in range(name_len-3) :
                        #        mat += l[h+3]
                        #        gMydB.gBLENAME_L = mat

                                        #if(start_flag == 1) :
                                        #       start_flag = 5
                                        #print('BLE NAME : %s') % (gMydB.gBLENAME_L)
                                        #val_text(mat, False)
                                        #print(l)
                                        #print(mat)
                                        #j = j+1

                 elif(i == parsor_ble_text[9]) :
                        #BLE RSSI
                        mat = l[1]
                        gMydB.gBLERSSI_L = mat
                        #print('RSSI : %s') % (gMydB.gBLERSSI_L)

                        LD_MAT.append(l[1])

                        #print('ble_event_type_flag : %d') % (ble_event_type_flag)
                        #print(LD_MAT)
			if(gMydB.gBLEMAC_L.find(mac_filter) == 0) : 
	                        #pdb.set_trace()

                                s = str(datetime.now())
                                gMydB.gBLETime = s

                                gMydB.gBLEMAC = gMydB.gBLEMAC_L
                                gMydB.gBLERSSI = gMydB.gBLERSSI_L
                                gMydB.gBLENAME = gMydB.gBLENAME_L
                                gMydB.gTXPower = gMydB.gTXPower_L
                                gMydB.gBatteryLevel = gMydB.gBatteryLevel_L
                                gMydB.gTemperature = gMydB.gTemperature_L
                                gMydB.gLDName = gMydB.gLDName_L
                                gMydB.gLDRSSI = str(gMydB.gLDRSSI_L)

                                # Find solubiz mac address
                                if(gMydB.gBLEMAC.find(mac_filter) == 0) :

                                        #pdb.set_trace()
                                        if(gMydB.gBLEMAC.find(mac_filter+':FF') == 0) :
                                                # get LD name
                                                par = gMydB.gBLEMAC.split(':')
                                                ttt = (int(par[4],16)*0x100 + int(par[5],16))
                                                gMydB.gBLENAME = ( format("LD%s") % str(ttt).zfill(5) )
                                                BLE_PRINT()
                                                gMydB.SQL_InsertBLE()         #ble_device insert
                                                #ble_event_type_flag = 0
                                                gMydB.gBLEMAC_L = ' '
                                                gMydB.gBLERSSI_L = ' '
                                                gMydB.gBLENAME_L = ' '
                                                gMydB.gTXPower_L = ' '
                                                gMydB.gBatteryLevel_L = ' '
                                                gMydB.gTemperature_L = ' '
                                                gMydB.gLDName_L = ' '
						gMydB.gLDRSSI_L = ' '

                                        else : 

                                                if(ble_event_type_flag == 4) : 					 
                                                        BLE_PRINT()
                                                        gMydB.SQL_InsertBLE()         #ble_device insert

                                                        gMydB.gBLEMAC_L = ' '
                                                        gMydB.gBLERSSI_L = ' '
                                                        gMydB.gBLENAME_L = ' '
                                                        gMydB.gTXPower_L = ' '
                                                        gMydB.gBatteryLevel_L = ' '
                                                        gMydB.gTemperature_L = ' '
                                                        gMydB.gLDName_L = ' '
		                                        gMydB.gLDRSSI_L = ' '

                                                        #ble_event_type_flag = 0

					         #else : 


        except : 
                #print('!!!!getBLEData Error!!!!')
		ha = 1

#===================================================================================

#======================================
def find_ble(s) :
        try :
               for i in parsor_ble_text :

                       u = s.find(i)

                       if(u > -1) :
                                l = s.split()
                                #print(l)
                                getBLEdata(i, l)
                                #pdb.set_trace()                # stop break
                                break;

                       #else :
                               #print(s)

        except :
                print("[Error] find_ble Error!!");
#======================================


#===================================================================================
def find_string(s) :
        try :

                #global gBLEMAC, gBLERSSI, gBLETime, gBLENAME, gTXPower, gBatteryLevel, gTemperature, gLDName
                #global gBLEMAC_L, gBLERSSI_L,  gBLENAME_L, gTXPower_L, gBatteryLevel_L, gTemperature_L, gLDName_L

                global start_flag, gLD_RSSI

                for i in parsor_text :
                        #range(k, parsor_text)
                        u = s.find(i)
                        if(u > -1) :
                                # Data find
                                l = s.split()
                                #print(l)

                                if(i == parsor_text[0]) :
                                        #Event type : ADV_IND & SCAN_RSP
                                        mat = l[5]
                                        #print('==============Start===========')
                                        ret = cmp('ADV_IND', mat)
                                        #print('ret : ', ret, start_flag)
                                        if(ret > -1) :
                                                #print('ADV_IND', mat, ret)
                                                start_flag = 0

                                        else :
                                                if(start_flag == 0) :
                                                        #print('SCAN_RSP', mat)
                                                        start_flag = 1
                                                else :
                                                        #print('NON_ADV_IND', mat)
                                                        start_flag = 3

                                elif(i == parsor_text[1]) :
                                        #MAC Address
                                        mat = l[1]
                                        gMydB.gBLEMAC_L = mat
                                        #print('MAC ADDRESS : %s') % (gMydB.gBLEMAC_L)
                                        #val_text(mat, False)
                                        #print(l)
                                        #print(mat)
                                        #j = j+1
                                elif(i == parsor_text[2]) :
                                        #TX Power
                                        mat = l[2]
                                        gMydB.gTXPower_L = mat
                                        #print('TX Power : %s') % (gMydB.gTXPower_L)

                                elif(i == parsor_text[3]) :
                                        #battery level
                                        mat = l[4]
                                        gMydB.gBatteryLevel_L = mat
                                        #gBatteryLevel_L = int(mat, 16)
                                        #print(l)
                                        #print('Battery Level : %s , start_flag : %d') % (gMydB.gBatteryLevel_L, start_flag)

                                elif(i == parsor_text[4]) :
                                        #temperature
                                        mat = l[4]
                                        gMydB.gTemperature_L = mat
                                        #gTemperature_L = int(mat, 16)
                                        #print(l)
                                        #print('Temperature : %s degree') % (gMydB.gTemperature_L)

                                elif(i == parsor_text[5]) :
                                        #Company
                                        mat = l[3]
                                        #print('Company:')
                                        #print(mat)
                                        #temp = hex(int(mat))
                                        #if(temp == 0x03E8) :
                                        #       print('LD')
                                        #       print(mat)
                                        #if(len(temp) == 6) :
                                                #print('Company:', mat)
                                                #stemp = temp[4:6] + temp[2:4]
                                                #mat = binascii.a2b_hex(stemp)
                                                #print (mat)
                                                #gLDName_L = mat

                                elif(i == parsor_text[6]) :
                                        #LD Data
                                        mat = l[1]

                                        mat1 = binascii.a2b_hex(mat)
                                        if( (len(mat1) >= 6) and (len(mat1) < 11) ) :
                                                #print (mat1)
                                                #print('data :',mat1)
                                                #gLDName_L = gLDName_L + mat1
						#pdb.set_trace()
                                                gMydB.gLDName_L = mat1[:-1]
						gLD_RSSI = int(mat[-2:],16) - 256	#string end character
						print(gMydB.gLDName_L, len(gMydB.gLDName_L))
						print('RSSI : ', gLD_RSSI)

                                elif(i == parsor_text[7]) :
                                        #BLE NAME

                                        name_len = len(l)

                                        mat = l[2]

                                        for h in range(name_len-3) :
                                                mat += l[h+3]

                                        gMydB.gBLENAME_L = mat
                                        #if(start_flag == 1) :
                                        #       start_flag = 5
                                        #print('BLE NAME : %s') % (gMydB.gBLENAME_L)
                                        #val_text(mat, False)
                                        #print(l)
                                        #print(mat)
                                        #j = j+1

                                elif(i == parsor_text[8]) :
                                        #BLE RSSI
                                        mat = l[1]
                                        gMydB.gBLERSSI_L = mat
                                        #print('RSSI : %s') % (gMydB.gBLERSSI_L)
                                        #val_text(mat, True)
                                        #print(l)
                                        #print(mat)
                                        #j = j+1
                                        if(start_flag == 0) :
                                                start_flag = 4          #HA171123
                                        elif(start_flag == 1) :
                                                start_flag = 5
                                        #print('SF :', start_flag)

                        #else :
                        #       print('ERRRR')

                #=============== print =================
                if(start_flag == 5) :
                #if(start_flag == 0) :

                        if(len(gMydB.gBLENAME_L) > 4) :
                                #print("Tag find :::: ")
                                s = str(datetime.now())
                                gMydB.gBLETime = s

                                gMydB.gBLEMAC = gMydB.gBLEMAC_L
                                gMydB.gBLERSSI = gMydB.gBLERSSI_L
                                gMydB.gBLENAME = gMydB.gBLENAME_L
                                gMydB.gTXPower = gMydB.gTXPower_L
                                gMydB.gBatteryLevel = gMydB.gBatteryLevel_L
                                gMydB.gTemperature = gMydB.gTemperature_L
                                gMydB.gLDName = gMydB.gLDName_L

                                # Find solubiz mac address
                                #if(gMydB.gBLEMAC.find('53:4F:4C') == 0) :
                                        #BLE_PRINT()
                                        #gMydB.SQL_InsertBLE()         #ble_device insert

                        gMydB.gBLEMAC_L = ' '
                        gMydB.gBLERSSI_L = ' '
                        gMydB.gBLENAME_L = ' '
                        gMydB.gTXPower_L = ' '
                        gMydB.gBatteryLevel_L = ' '
                        gMydB.gTemperature_L = ' '
                        gMydB.gLDName_L = ' '
                        start_flag = 2


                elif(start_flag == 3) :
                        gMydB.gBLEMAC_L = ' '
                        gMydB.gBLERSSI_L = ' '
                        gMydB.gBLENAME_L = ' '
                        gMydB.gTXPower_L = ' '
                        gMydB.gBatteryLevel_L = ' '
                        gMydB.gTemperature_L = ' '
                        gMydB.gLDName_L = ' '
                        start_flag = 2

                elif(start_flag == 4) :
                #else :
                        #print('4.find_staring flag = %d') % ( start_flag )

                        #if(len(gBLENAME_L) > 4) :
                        if 1 :
                                s = str(datetime.now())
                                gMydB.gBLETime = s

                                gMydB.gBLEMAC = gMydB.gBLEMAC_L
                                gMydB.gBLERSSI = gMydB.gBLERSSI_L
                                gMydB.gBLENAME = gMydB.gBLENAME_L
                                gMydB.gTXPower = gMydB.gTXPower_L
                                gMydB.gBatteryLevel = gMydB.gBatteryLevel_L
                                gMydB.gTemperature = gMydB.gTemperature_L
                                gMydB.gLDName = gMydB.gLDName_L

                                # Find solubiz mac address
                                if(gMydB.gBLEMAC.find('53:4F:4C') == 0) :

                                        if(gMydB.gBLEMAC.find('53:4F:4C:FF') == 0) :
                                                # get LD name
                                                par = gMydB.gBLEMAC.split(':')
                                                ttt = (int(par[4],16)*0x100 + int(par[5],16))
                                                gMydB.gBLENAME = ( format("LD%s") % str(ttt).zfill(5) )

                                        #else :
                                                # get ble name
                                        #        ret = gMydB.checkMOLDLIST(gMydB.gBLEMAC)
                                        #        gMydB.gBLENAME = ret[1]
                                        	BLE_PRINT()
                                        #gMydB.SQL_InsertBLE()         #ble_device insert
						gLD_RSSI = 0



                        gMydB.gBLEMAC_L = ' '
                        gMydB.gBLERSSI_L = ' '
                        gMydB.gBLENAME_L = ' '
                        gMydB.gTXPower_L = ' '
                        gMydB.gBatteryLevel_L = ' '
                        gMydB.gTemperature_L = ' '
                        gMydB.gLDName_L = ' '
                        start_flag = 2


        except :
                #print('find strig Error')
		HA = 1

#======================================
def BTMON_START() :

        try :
                p = Popen(['btmon'], shell=True, stdout=PIPE)
                time_flag = 0;  #HA170207 time_flag == 0 time is ok else sleep
                while True :
                        ERROR_LED(False)
                        # ===================== HA170207 ADD ===========================
                        if(time_flag == 0) :
                                print('setting time')
                                timegap = timedelta(seconds=60)
                                gTimeCompare = datetime.now()
                                comparetime = gTimeCompare + timegap
                                time_flag = 1

                        temp = datetime.now()
                        # =============================================================
                        #print('temp time : ', temp)
                        #print('comparetime time : ', comparetime)

                        if(temp < comparetime) :

                                p.poll()
                                data = p.stdout.readline()
                                #print('================================================')
                                if(data) :
                                        #print data
                                        #find_string(data)
                                        queue.append(data)
                                        find_ble(queue.popleft())

                        else :
                                print('Sleep period')
                                #time.sleep(60*29)        # 1min * 4 = 4min sleep
                                time.sleep(1)        # 1min * 4 = 4min sleep
                                time_flag = 0           # time floag initial
        except :
                print('BTMON Start Error')

#======================================
#======================================

def LED_INIT() :
        try :
                #GPIO.cleanup()

                GPIO.setmode(GPIO.BCM)
                GPIO.setup(27, GPIO.OUT)
                GPIO.output(27, True)
                GPIO.setup(22, GPIO.OUT)
                GPIO.output(22, True)
                GPIO.setup(17, GPIO.OUT)
                GPIO.output(17, True)
        except :
                print('LED_Init Error')

#======================================

def PWR_LED(ONOFF) :
        try :
                if(ONOFF) :
                        GPIO.output(27, False)
                        #print("Power LED ON")
                else :
                        GPIO.output(27, True)
                        #print("Power LED OFF")
        except :
                print('PWR LED Error')

#======================================

def STATUS_LED(ONOFF) :
        try :
                if(ONOFF) :
                        GPIO.output(22, False)
                        #print("Status LED ON")
                else :
                        GPIO.output(22, True)
                        #print("Status LED OFF")
        except :
                print('Status LED Error')

#======================================

def ERROR_LED(ONOFF) :
        try :
                if(ONOFF) :
                        GPIO.output(17, False)
                        #print("Error LED ON")
                else :
                        GPIO.output(17, True)
                        #print("Error LED OFF")
        except :
                print('Error LED Error')

#======================================

def LED_BLINK() :
        try :
                STATUS_LED(True)
                time.sleep(1)
                STATUS_LED(False)
                time.sleep(1)
        except :
                print('LED Blink Error')

#======================================

def LED_main() :
        try :
                # variable init
                LED_INIT()
                PWR_LED(True)
                while(1) :
                        LED_BLINK()

                GPIO.cleanup()
        except :
                print('LED_main Error')

#======================================

#======================================
def BT_main() :

        try :
                global gMydB

                print('New anybin 2017 :: V1.0')
                a = CreatedBTable()

                gMydB = a

                a.SQL_MSSQL_OPEN()

                a.SQL_getNEWMoldLoc()     # get the mold_loc for YWIC_MMS_NEW

                #a.ShowTables()
                ret1 = a.DROPTABLES()
                print(ret1)

                ret = a.CREATE_TABLES()
                print( len(ret), ret)

                if(ret[0] >= 0) :
                        a.SQL_BLEGATEWAY_INIT()

                #if(ret[1] >= 0) :
                        # ble_device data insert
                        #a.SQL_INPUTDATA()                      #ble_device information input
                        #a.SQL_InsertBLE()

                BTMON_START()

        except :
                print ('ble main :: Error')
                ERROR_LED(True)

        finally :
                a.SQL_DELETE()
                print('ble main normal close')

#======================================
def main() :

        try :
                # variable init
                print("==========================================================")
                print("       [ Red bi:n ] Ver 1.6 - BLE Scanner                 ")
                print("==========================================================")

                th1 = threading.Thread(target=LED_main)
                th1.start()

                th2 = threading.Thread(target=BT_main)
                th2.start()

        except :
                print("!!!!!!!!!!!!!!!Thread Error!!!!!!!!!!!!!")
                th1.join()
                th2.join()


#======================================
if __name__ == '__main__' :

        try :
                main()
        except KeyboardInterrupt :
                print("BLE END")
#======================================

