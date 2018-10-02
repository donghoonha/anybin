#!/usr/bin/env python
# -*- coding: cp949 -*-

#=============================================================================================
# BLE Beacon get Data Function
# Author : HADES
# Date : 2018. 07. 25
#=============================================================================================

import os
import time
import sys

from threading import Thread, Lock

from collections import deque

from datetime import date, datetime, timedelta

from subprocess import *

import binascii

import pdb

from pandas import Series, DataFrame
import pandas as pd
import numpy as np

import Adafruit_DHT as dht

from pympler.tracker import SummaryTracker
tracker = SummaryTracker()

from JSON_KETI import *

excel_db_file = '/home/pi/Anybin/KETI/excel_db_file.xlsx'

#============================================ Class Start ===========================================
class anybin_read : 

	df = DataFrame()
	df1 = DataFrame()
	df6 = DataFrame()
	df5 = DataFrame()
	df_backup = DataFrame()

	df_excel = DataFrame()

	df_index = 0
	ddf_index = 0
	LD_MAT = []
	queue = deque([])

	parsor_ble_text = ['> HCI Event:','Event type:','Address:','TX power','Name (complete):','Service Data (UUID 0x180f):','Service Data (UUID 0x1809):','Service Data (UUID 0x2a98):','Service Data (UUID 0x2a9a):','Data:','RSSI:']
	ble_event_type = ['ADV_IND','ADV_DIRECT_IND','ADV_NONCONN_IND','SCAN_RSP']
	ble_data_flag = 0
	ble_event_type_flag = 0

	#======================================
	def __init__(self) : 

		print('[__init__] anybin_read class start')
		self.var_init()

	#======================================
	def var_init(self) : 
		self.df = DataFrame([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0], index=([self.parsor_ble_text[0],self.parsor_ble_text[1],self.parsor_ble_text[2],self.parsor_ble_text[3],self.parsor_ble_text[4],self.parsor_ble_text[5],self.parsor_ble_text[6],self.parsor_ble_text[7],self.parsor_ble_text[8],self.parsor_ble_text[9],self.parsor_ble_text[10],self.parsor_ble_text[0]+'1',self.parsor_ble_text[1]+'1',self.parsor_ble_text[2]+'1',self.parsor_ble_text[10]+'1','DATE:']))
		self.df6 = DataFrame([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0], index=([self.parsor_ble_text[0],self.parsor_ble_text[1],self.parsor_ble_text[2],self.parsor_ble_text[3],self.parsor_ble_text[4],self.parsor_ble_text[5],self.parsor_ble_text[6],self.parsor_ble_text[7],self.parsor_ble_text[8],self.parsor_ble_text[9],self.parsor_ble_text[10],self.parsor_ble_text[0]+'1',self.parsor_ble_text[1]+'1',self.parsor_ble_text[2]+'1',self.parsor_ble_text[10]+'1','DATE:']))
		self.df1 = self.df.copy()
		#self.df_backup = self.df.copy()

	#======================================
	def read_excel(self) : 
		#self.df_excel = pd.read_excel('Device List_2018-08-23T07_33_59.480Z.xlsx')		
		self.df_excel = pd.read_excel(gEXCEL_FILE)
		print self.df_excel

		try : 
			self.df_backup = pd.read_excel(excel_db_file)
		except : 
			print('[Error] excel_db_file read error')
		print self.df_backup

	#======================================
	def save_excel(self) : 
		try : 
			# Create a Pandas Excel writer using XlsxWriter as the engine.
			writer = pd.ExcelWriter(excel_db_file, engine='xlsxwriter')

			# Convert the dataframe to an XlsxWriter Excel object.
			self.df_backup.to_excel(writer, sheet_name='ws-200')

			# Close the Pandas Excel writer and output the Excel file.
			writer.save()
		
			print('[Write] OK !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
		except : 
			print('[Error] excel_db_file writing error')

	#======================================
	def START(self, op_time, sleep_time) : 
		self.BTMON_START(op_time, sleep_time)	# BTMON Start		

	#======================================
	def getBLEdata(self, i, l) : 
		try : 
			if(i == self.parsor_ble_text[0]) :
				# > HCI Event: get Time
				del self.LD_MAT[:]

				self.LD_MAT.append(l[10])
				self.df.loc[i] = l[10]

			elif(i == self.parsor_ble_text[1]) :
				# Event Type: ADV_IND(0x00), ADV_DIRECT_IND(0x02), ADV_NONCONN_IND(0x03), SCAN_RSP(0x04)
				#print(i, l[5], l[6])		#first start

				if(l[5] == self.ble_event_type[0]) :
					# ADV_INV(0x00)
					#print(i, l[5])
					#LD_MAT[1] = l[5]
					self.ble_event_type_flag = 0

					temp = self.df.loc[self.parsor_ble_text[0]]
					self.df[0] = ([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
					self.df.loc[self.parsor_ble_text[0]] = temp

					self.LD_MAT.append(l[5])
					self.df.loc[i] = l[5]

				elif(l[5] == self.ble_event_type[3]) :
					# SCAN_RSP(0x04)
					#print(i, l[5])
					#LD_MAT[1] = l[5]
					self.ble_event_type_flag = 4

					self.df1 = self.df	# ADV_IND data copy

					self.LD_MAT.append(l[5])
					self.df1.loc[i+'1'] = l[5]

				if(l[6] == self.ble_event_type[2]) : 
					# ADV_NONCONN_IND(0x03)
					#print(i, l[6])
					#LD_MAT[1] = l[6]
					self.ble_event_type_flag = 3

					temp = self.df.loc[self.parsor_ble_text[0]]
					self.df[0] = ([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
					self.df.loc[self.parsor_ble_text[0]] = temp

					self.LD_MAT.append(l[6])
					self.df.loc[i] = l[6]

			elif(i == self.parsor_ble_text[2]) :
				# MAC Address
				#print(i, l[1])
				#LD_MAT[2] = l[1]
				self.LD_MAT.append(l[1])

				#if(ble_event_type_flag == 4) :
				#	df1.loc[i+'1'] = l[1]
				#else :
				#	df.loc[i] = l[1]
				self.df.loc[i] = l[1]

			elif(i == self.parsor_ble_text[3]) :
				# Tx Power
				#print(i, l[2])
				#LD_MAT[3] = l[2]
				self.LD_MAT.append(l[2])
				self.df.loc[i] = l[2]

			elif(i == self.parsor_ble_text[4]) :
				# Name
				#print(i, l[2])
				#LD_MAT[4] = l[2]
				self.LD_MAT.append(l[2])
				self.df.loc[i] = l[2]

			elif(i == self.parsor_ble_text[5]) :
				# serivce uuid 0x180f : battery level
				#print(i, l[4])
				#LD_MAT[5] = l[4]
				self.LD_MAT.append(l[4])
				self.df.loc[i] = l[4]

			elif(i == self.parsor_ble_text[6]) :
				# serivce uuid 0x1809 : temperature level
				#print(i, l[4])
				#LD_MAT[6] = l[4]
				self.LD_MAT.append(l[4])
				self.df.loc[i] = l[4]

			elif(i == self.parsor_ble_text[7]) :
				# serivce uuid 0x2a98 : weight level x 5
				#print(i, l[4])
				#LD_MAT[7] = l[4]
				self.LD_MAT.append(l[4])
				self.df.loc[i] = l[4]

			elif(i == self.parsor_ble_text[8]) :
				# serivce uuid 0x2a9a : user define
				#print(i, l[4])
				#LD_MAT[8] = l[4]
				self.LD_MAT.append(l[4])
				self.df.loc[i] = l[4]

			elif(i == self.parsor_ble_text[9]) :
				# Company user data 
				#print(i, l[1])
				#LD_MAT[9] = l[1]
				self.LD_MAT.append(l[1])
				self.df.loc[i] = l[1]

			elif(i == self.parsor_ble_text[10]) :
				# RSSI : BLE RSSI level
				#print(i, l[1])
				#LD_MAT[10] = l[1]
				self.LD_MAT.append(l[1])

				if(self.ble_event_type_flag == 4) :
					self.df1.loc[i+'1'] = l[1]
					self.df = self.df1
				else :
					self.df.loc[i] = l[1]

				if(self.ble_event_type_flag == 4 ) :
				
					print(self.ddf_index, self.LD_MAT)
					self.ddf_index = self.ddf_index + 1
					#print(df)
			 
				#if(self.ble_event_type_flag == 4) :

					# Datetime
					self.df.loc['DATE:'] = str(datetime.now())
			

					if(self.df_index == 0) : 
						self.df6[self.df_index] = self.df
						self.df_index = self.df_index + 1

					else : 			

						# df6 data Gather
						A = self.df6[self.df6==self.df.loc[self.parsor_ble_text[2],0]].stack().index.get_level_values(1)

						if(A >= 0) :
							# Data update
							self.df6[A] = self.df
						else :
							# new Data insert
							self.df6[self.df_index] = self.df
							self.df_index = self.df_index + 1
					#print(df6)
			
		
		except : 
			print('[Error] getBLEdata Error')
	#======================================

	#======================================
	def find_ble(self, s) : 
		try : 
			for i in self.parsor_ble_text : 

				u = s.find(i)

				if(u > -1) : 
					l = s.split()
					#print(l)
					self.getBLEdata(i, l)
					#pdb.set_trace()		# stop break
					break;

				#else : 
					#print(s)

		except : 
			print("[Error] find_ble Error!!");
	#======================================
	def DHT22_READ(self) :
		try : 
			h,t = dht.read_retry(dht.DHT22, 10)	# Gpio 10 connect
			print 'Temp={0:0.2f} C, Humidity={1:0.2f} %'.format(t,h)
			return(h,t)

		except : 
			print('[Error] DHT22_READ Error')

	#======================================
	def data_queue(self) : 
		try : 
			while(1) : 
				if(len(self.queue)) : 
					self.find_ble(self.queue.popleft())

		except : 
			print('[Error] data_queue Error')	

	def get_weight(self, s) : 
		try : 
			b = s[2]+s[3]+s[0]+s[1]
			c = int(b, 16)
			if(c > 0x8000) :
				weight = (c - 0x10000)*5
			else : 
				weight = c*5
			#print('s : ', weight)
			return weight

		except : 
			print('[Error] get_weight Error')

	#======================================
	def BTMON_START(self, op_time, sleep_time) :
	        try :
        	        p = Popen(['btmon'], shell=True, stdout=PIPE)
                	time_flag = 0;  #HA170207 time_flag == 0 time is ok else sleep
	                while True :
                	        # ===================== HA170207 ADD ===========================
	                        if(time_flag == 0) :
        	                        print('setting time')
                	                timegap = timedelta(seconds=op_time)
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
						#self.find_ble(data)
		                	        #print('temp End time : ', datetime.now())

						# queue data input
						#pdb.set_trace()		# stop break

						self.queue.append(data)
						self.find_ble(self.queue.popleft())
						#print(data)
						#time.sleep(0.1)
			
	                        else :
        	                        print('Sleep period')
	
					TS = datetime.now()

					# df6 sort 
					self.df5 = self.df6.T

					if( self.df5['Address:'][0] == 0 ) :
						pdb.set_trace() 
						#del(self.df5.loc[0])
						df9 = self.df5[1:]
						self.df5 = df9.reset_index(drop=1)

					#self.df5 = self.df5.sort_values([parsor_ble_text[2]], axis=1, ascending=False)
					df4 = self.df5.sort_values(by=['Address:'])
					self.df5 = df4.reset_index(drop=1)
					print("*" * 50)
					with pd.option_context('display.max_rows', None, 'display.max_columns', None) :
						print(self.df5)
					print(self.df5.shape)
					print("-" * 50)

					TE = str(datetime.now() - TS)
					print("self.df5 Calc Time : ", TE)

					try : 				

						(humidity, temperature) = self.DHT22_READ()		# Humidity & Temperature Read
	
						#pdb.set_trace()
	
						T1 = datetime.now()
						Gateway_XML_Send(gGATEWAY_DID, str(temperature), str(humidity), str(datetime.now()))				# IoT Platform send data
						T2 = datetime.now()	
					except : 
						print('[Error] DHT22_READ & Gateway_XML_Send Error')

					#print(self.df5['Address:'])

					my_mac = '53:4F:4C:88'

					pp = DataFrame()
					pp1 = DataFrame()
					print '$001'
					print self.df5
					df5_index = self.df5['Address:'].str.contains(my_mac)
					print df5_index
					df5_in = df5_index.dropna()
					print df5_in
					#pp =  self.df5[self.df5['Address:'].str.contains(my_mac)].stack().index.get_level_values(0)
					pp =  self.df5[df5_in].stack().index.get_level_values(0)
					print '$002'
					pp1 = pp.unique()
					print '$003'
					print pp, pp1

					if(self.df_backup.shape[0] > 0) : 

						for i in pp1 : 
							try : 
								aa = self.df5['Address:'][i]
								weight = self.get_weight(self.df5[self.parsor_ble_text[7]][i])
								pp4 = 0
								pp5 = 0
								weight_old = 0

								try :
									pp4 =  self.df_backup[self.df_backup['Address:'].str.contains(aa)].stack().index.get_level_values(0)
									pp5 = pp4.unique()
									print pp5[0]
								except : 
									#pdb.set_trace()

									self.df_backup.loc[self.df_backup.shape[0]] = self.df5.loc[i].copy()

									pp4 =  self.df_backup[self.df_backup['Address:'].str.contains(aa)].stack().index.get_level_values(0)
									pp5 = pp4.unique()

									print 'df_backup Error : new data add', aa, weight, weight_old									

								try : 
									weight_old = self.get_weight(self.df_backup[self.parsor_ble_text[7]][pp5[0]])
									print aa, weight, weight_old
								except : 
									# df_backup no data, add data
									#pdb.set_trace()
										
									#self.df_backup.loc[self.df_backup.shape[0]] = self.df5.loc[i].copy()
									#weight_old = 0
									print 'get weight Error', aa, weight, weight_old

								if( np.absolute(weight - weight_old) < 20 ) : 
									#old data send
									try : 
										self.df5[self.parsor_ble_text[7]][i] = self.df_backup[self.parsor_ble_text[7]][pp5[0]]
										weight = weight_old
										#print weight
									except : 
										print('[Error] df_backup weight Error')
								else : 
									# 20g over weight
									self.df_backup[self.parsor_ble_text[7]][pp5[0]] = self.df5[self.parsor_ble_text[7]][i]

								print weight	
								#bb = 'D{0}{1}{2}{3}'.format(aa[12],aa[13],aa[15],aa[16])
								col = self.df_excel.columns
								#pp2 =  self.df_excel[self.df_excel[col[2]].str.contains(bb)].stack().index.get_level_values(0)
								pp2 =  self.df_excel[self.df_excel[col[9]].str.contains(aa)].stack().index.get_level_values(0)
								pp3 = pp2.unique()
								UID = self.df_excel.loc[pp3[0]][2]
								AEID = self.df_excel.loc[pp3[0]][4]
								print pp3, UID, AEID
								#Device_XML_Send('D0001', 'S000000000033154854257', '10.12', str(datetime.now()))	# IoT Platform send data
								Device_XML_Send(UID, AEID, str(weight), str(datetime.now()))	# IoT Platform send data

	
							except : 
								print('[Error] No Tag comp')
						
							#pdb.set_trace()


					else : 
						# first process
						# get index data
						for i in pp1 : 
							try : 
								aa = self.df5['Address:'][i]
								weight = self.get_weight(self.df5[self.parsor_ble_text[7]][i])
								print aa, weight
								#bb = 'D{0}{1}{2}{3}'.format(aa[12],aa[13],aa[15],aa[16])
								col = self.df_excel.columns
								#pp2 =  self.df_excel[self.df_excel[col[2]].str.contains(bb)].stack().index.get_level_values(0)
								pp2 =  self.df_excel[self.df_excel[col[9]].str.contains(aa)].stack().index.get_level_values(0)
								pp3 = pp2.unique()
								UID = self.df_excel.loc[pp3[0]][2]
								AEID = self.df_excel.loc[pp3[0]][4]
								print pp3, UID, AEID
								#Device_XML_Send('D0001', 'S000000000033154854257', '10.12', str(datetime.now()))	# IoT Platform send data
								#Device_XML_Send(UID, AEID, str(weight), str(datetime.now()))	# IoT Platform send data

	
							except : 
								print('[Error] No Tag')
						
							#pdb.set_trace()

						# first data copy
						self.df_backup = self.df5.copy()


					#Device_XML_Send('D0001', 'S000000000033154854257', '10.12', str(datetime.now()))	# IoT Platform send data
					T3 = datetime.now()
					#print(str(T2-T1), str(T3-T2), str(T3-T1))

					#tracker.print_diff()	# memory tracker

                        	        time_flag = 0           # time floag initial

					self.ddf_index = 0		#imsi

					# data delete and copy
					#with pd.option_context('display.max_rows', None, 'display.max_columns', None) :
					print self.df5
					print 'self.df5'
					
					print self.df_backup
					print 'self.df_backup'
					#self.df_backup = self.df5.copy()
					self.save_excel()

					del(self.df, self.df1, self.df5, self.df6)
					self.var_init()
					self.df_index = 0

                	                time.sleep(sleep_time)        # 1min * 4 = 4min sleep

	        except :
        	        print('[Error] BTMON Start Error')

	#======================================



#============================================ Class End =============================================


#======================================
def main() :

        try :
                # variable init
                print("==========================================================")
                print("       [ White bi:n ] Ver 1.7 - BLE Scanner               ")
                print("==========================================================")

		A = anybin_read()
		A.read_excel()
		A.START(40, 10)

		#arg = (50, 10)
		#t_s = Thread(target=A.BTMON_START, args = arg)
		#t_s.daemon = True
		#t_s.start()

		#pdb.set_trace()		# stop break

		#t_g = Thread(target=A.data_queue)
		#t_g.daemon = True
		#t_g.start()


		#BTMON_START()
		#read_ble()
                #th2 = threading.Thread(target=BT_main)
                #th2.start()

        except :
                print("!!!!!!!!!!!!!!!Thread Error!!!!!!!!!!!!!")
                #t_s.join()
                #t_g.join()


#======================================
if __name__ == '__main__' :

        try :
                main()
        except KeyboardInterrupt :
                print("ANYBIN_READ END")
#======================================

