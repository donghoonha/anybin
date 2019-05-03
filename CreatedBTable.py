#!/usr/bin/env python
# -*- coding: cp949 -*-

#=============================================================================================
# database new create for each daily table
# old : ble_device -> new ble_device_170801 modify
# if not exist and create table
#=============================================================================================

import os
import time, threading
import MySQLdb

from datetime import date, datetime, timedelta

import pymssql
import math

from subprocess import *

# today
table_name = 'ble_device_' + str(date.today()).split()[0].replace('-', '')
# yesterday
table_name_old = 'ble_device_' + str(date.today() - timedelta(1)).split()[0].replace('-', '')

TABLES = {}
TABLES[table_name] = (
        "create table " + table_name + " ("
        #"blackbin_index integer not null auto_increment,"
        "blackbin_index integer not null identity(1,1),"
        "blackbin_bs_name varchar(80),"
        "blackbin_name varchar(80),"
        "blackbin_macaddress varchar(80),"
        "blackbin_rssi varchar(8),"
        "blackbin_batterylevel varchar(8),"
        "blackbin_receivedatetime varchar(80),"
        "PRIMARY KEY(blackbin_index)"
        ");")

TABLES['ble_scanner'] = (
        "create table ble_scanner ("
        #"bs_index integer not null auto_increment,"
        "bs_index integer not null,"
        "bs_name varchar(80),"
        "bs_ipaddress varchar(80),"
        "bs_constraincondition varchar(80),"
        "bs_productco varchar(80),"
        "bs_managelocation varchar(80),"
        "bs_locX varchar(8),"
        "bs_locY varchar(8),"
        "bs_locZ varchar(8),"
        "bs_datetime varchar(80),"
        "PRIMARY KEY(bs_index)"
        ") ;")


class CreatedBTable :

        # db, curs variable create
        db = ' '
        curs = ' '
        # ble_device member variable
        gSCANNER_ID = ' '
        gBLENAME = ' '
        gBLEMAC = ' '
        gBLERSSI = ' '
        gBatteryLevel = ' '
        gBLETime = ' '
        gTemperature = ' '
        gTXPower = ' '
        gLDName = ' '
        gLDRSSI = ' '

        gBLENAME_L = ' '
        gBLEMAC_L = ' '
        gBLERSSI_L = ' '
        gBatteryLevel_L = ' '
        gTemperature_L = ' '
        gTXPower_L = ' '
        gLDName_L = ' '
        gLDRSSI_L = ' '

        #ble_scanner member variable
        gSqlIndex = ' '
        gGatewayName = ' '
        gGatewayIP = ' '
        gIPAddress = ' '
        gSCANNER_ID = ' '
        gConstrain = ' '
        gProductco = ' '
        gLocation = ' '
        gLocx = ' '
        gLocy = ' '
        gLocz = ' '
        gDatetime = ' '

        #database info
        gServerIP = '222.100.204.221:1433'
        gLoginID = 'mold-admin'
        gPassword = 'a098123'
        gDBNAME = 'YW_MES_NEW'

        data_MoldLoc = []


        def __init__(self) :
                print('CreatedBTable init')
                self.Read_File()

        def Read_File(self) :
                #global gServerIP, gLoginID, gPassword, gDBNAME
                #global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime
                parsor_t = ['serverIP:', 'loginID:','password','dbname', 'ipaddress:', 'scannerid:', 'constrain:', 'productco:', 'lcation:', 'locx:', 'locy:', 'locz:']

                try :
                        f = open('/home/pi/Anybin/HBLE/anybin_gm_env', 'rb')
                        #f = open('./anybin_yonwoo_env', 'rb')

                        self.gDatetime = str(datetime.now())

                        for i in parsor_t :
                                line = f.readline()
                                l = line.split()

                                if(i == parsor_t[0]) :
                                        # db serverIP
                                        mat = l[1]
                                        self.gServerIP = mat
                                        print(mat)
                                elif(i == parsor_t[1]) :
                                        # db loginid
                                        mat = l[1]
                                        self.gLoginID = mat
                                        print(mat)
                                elif(i == parsor_t[2]) :
                                        # db password
                                        mat = l[1]
                                        self.gPassword = mat
                                        print(mat)
                                elif(i == parsor_t[3]) :
                                        # db name
                                        mat = l[1]
                                        self.gDBNAME = mat
                                        print(mat)
                                elif(i == parsor_t[4]) :
                                        # ipaddress
                                        mat = l[1]
                                        self.gIPAddress = mat
                                        print(mat)
                                elif(i == parsor_t[5]) :
                                        # scannerid
                                        mat = l[1]
                                        self.gSCANNER_ID = mat
                                        print(mat)
                                elif(i == parsor_t[6]) :
                                        # constrain
                                        mat = l[1]
                                        self.gConstrain = mat
                                        print(mat)
                                elif(i == parsor_t[7]) :
                                        # productco
                                        mat = l[1]
                                        self.gProductco = mat
                                        print(mat)
                                elif(i == parsor_t[8]) :
                                        # location
                                        mat = l[1]
                                        self.gLocation = mat
                                        print(mat)
                                elif(i == parsor_t[9]) :
                                        # locx
                                        mat = l[1]
                                        self.gLocx = mat
                                        print(mat)
                                elif(i == parsor_t[10]) :
                                        # locy
                                        mat = l[1]
                                        self.gLocy = mat
                                        print(mat)
                                elif(i == parsor_t[11]) :
                                        # locz
                                        mat = l[1]
                                        self.gLocz = mat
                                        print(mat)

                except IndexError as e:
                        print(e)

                finally :
                        f.close()

        #======================================

        def MYSQL_OPEN(self) :
                print(self.gServerIP, self.gLoginID, self.gPassword, self.gDBNAME)
                self.db = MySQLdb.connect(self.gServerIP, self.gLoginID, self.gPassword, self.gDBNAME)
                self.curs = self.db.cursor()
                print(self.db, self.curs)

        def SQL_MSSQL_OPEN(self) :

                ret = 0

                for i in range(1) :

                        try :
                                self.db = pymssql.connect(host = self.gServerIP, user = self.gLoginID, password = self.gPassword, database = self.gDBNAME)
                                self.curs = self.db.cursor()
                                ret = 0
                                print(self.db, self.curs)
                                return (ret)
                                break;

                        except pymssql.DatabaseError, e :
                                print('SQL_MSSQL_OPEN Error : ',i, e)
                                ret = -1
                                #self.gServerIP = '222.100.204.221'       # Server IP
                                time.sleep(0.2)

                        except :
                                print "Another Exception occured, most likely User or signon incorrect"
                                time.sleep(0.2)

        #======================================

        def CREATE_TABLES(self) :
                ret = []
                #print TABLES
                for name, dd1 in TABLES.iteritems() :
                        try :
                                #print("Creating table {}: ".format(name))
                                print dd1
                                self.curs.execute(dd1)
                                self.db.commit()
                                ret.append(0)
                                self.INITTABLE()

                        except pymssql.DatabaseError, e :
                                try:
                                        # if mssql then check error
                                        if(e.args[0] == 1050) :
                                                print('already exists.')
                                                ret.append(1)
                                        else :
                                                print "MSSQL Error [%d]: %s" % (e.args[0], e.args[1])
                                                ret.append(2)
                                except IndexError:
                                        print "MSSQL Error: %s" % str(e)
                                        ret.append(-1)

                return (ret)

        def ShowTables(self) :
                try :
                        self.curs.execute("show tables;")
                        for r in self.curs.fetchall() :
                                print(r)
                        self.curs.execute("SELECT FOUND_ROWS();")
                        for r in self.curs.fetchall() :
                                print(r)
                        if(r[0] >= 2L) :
                                print(r[0])
                                self.DROPTABLES()
                        else :
                                print('one table')
                except :
                        print('ShowTables Error :: Unknown table')

        def INITTABLE(self) :
                try :
                        SQL = "DBCC CHECKIDENT(" + table_name + ",RESEED, 0);"
                        #print SQL
                        self.curs.execute(SQL)
                        #self.db.commit()               # This function Needs
                        #for r in self.curs.fetchall() :
                        #       print(r)
                except :
                        print('DBCC CHECKIDENT Error :: Unknown table')


        def DROPTABLES(self) :
                try :
                        #print table_name_old
                        SQL = "drop table " + table_name_old + ";"
                        #print SQL
                        self.curs.execute(SQL)
                        self.db.commit()                # This function Needs
                        #for r in self.curs.fetchall() :
                        #       print(r)
                except :
                        print('DROPTABLES Error :: Unknown table')

        def SQL_InsertBLE(self) :
                try :
                        #global gSCANNER_ID, gBLENAME, gBLEMAC, gBLERSSI, gBatteryLevel, gBLETime, gTemperature, gTXPower

                        #print('************************************SQL_InsertBLE()******************************')

                        query = "INSERT INTO " + table_name +" (blackbin_bs_name, blackbin_name, blackbin_macaddress, blackbin_rssi, blackbin_batterylevel, blackbin_receivedatetime) VALUES(%s, %s, %s, %s, %s, %s)"

                        #print query
                        #self.curs.execute(query, (self.gSCANNER_ID, self.gBLENAME, self.gBLEMAC, self.gBLERSSI, self.gBatteryLevel, self.gBLETime))


                        if(self.gBLENAME.find('LD') == 0) :
                                self.curs.execute(query, (self.gSCANNER_ID, self.gBLENAME, self.gLDName, self.gLDRSSI, self.gBatteryLevel, self.gBLETime))
                        else :
                                self.curs.execute(query, (self.gSCANNER_ID, self.gBLENAME, self.gBLEMAC, self.gBLERSSI, self.gBatteryLevel, self.gBLETime))

                        #self.db.commit()

                except :
                        print('SQL_InsertBLE Error :: Unknown table')


        def SQL_INPUTDATA(self) :
                self.gSCANNER_ID = 'Anybi:n001'
                self.gBLENAME = 'AA001A'
                self.gBLEMAC = '53:4F:4C:11:11:11'
                self.gBLERSSI = '-59'
                self.gBatteryLevel = '100'
                self.gBLETime = str(datetime.now())

        #=============================================================================

        def SQL_AddGatewayInfo(self) :
                try :
                        self.gDatetime = str(datetime.now())

                        query = "INSERT INTO ble_scanner (bs_name, bs_ipaddress, bs_constraincondition, bs_productco, bs_managelocation, bs_locX, bs_locY, bs_locZ, bs_datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        self.curs.execute(query, (self.gSCANNER_ID,  self.gIPAddress, self.gConstrain, self.gProductco, self.gLocation, self.gLocx, self.gLocy, self.gLocz, self.gDatetime))

                        print query

                except :
                        print('SQL_AddGatewayInfo Error :: Unknown table')

        #======================================

        def SQL_UpdateGatewayInfo(self) :
                try :
                        query = "UPDATE ble_scanner SET bs_ipaddress=%s, bs_constraincondition=%s, bs_productco=%s, bs_managelocation=%s, bs_locX=%s, bs_locY=%s, bs_locZ=%s, bs_datetime=%s WHERE bs_name=%s"
                        self.curs.execute(query, (self.gIPAddress, self.gConstrain, self.gProductco, self.gLocation, self.gLocx, self.gLocy, self.gLocz, self.gDatetime, self.gSCANNER_ID))
                        #for r in curs.fetchall() :
                        #        print(r)
                except :
                        print('SQL_UpdateGatewayInfo Error :: Unknown table')


        #======================================

        def SQL_getCountGateway(self) :
                try :
                        #global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime

                        #print 'SQL_getCountGateway : ',  CreatedBTable.gSCANNER_ID

                        query = "SELECT COUNT(bs_name) from ble_scanner where bs_name = '%s'" % self.gSCANNER_ID

                        print query

                        try :
                                self.curs.execute(query)

                        except MySQLdb.Error, e:
                                try :
                                        print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                                except IndexError:
                                        print "MySQL Error: %s" % str(e)


                        #print ('ERRRRRRRR;')

                        for r in self.curs.fetchall() :
                                print('SQL_getCountGateway', r)
                        a = r[0]
                        print(r, a)
                        return(int(a))

                except :
                        print('SQL_getCountGateway Error :: Unknown table')

        #======================================

        def SQL_getGatewayIndex(self) :
                try :
                        #global gGatewayName, gGatewayIP
                        #global gIPAddress, gSCANNER_ID, gConstrain, gProductco, gLocation, gLocx, gLocy, gLocz, gDatetime

                        query = "select bs_Index from ble_scanner where bs_name = %s"
                        self.curs.execute(query, (self.gSCANNER_ID))

                        print query

                        for r in self.curs.fetchall() :
                                print(r)
                        a = r[0]
                        return(int(a))
                except :
                        print('SQL_getGatewayIndex Error :: Unknown table')

        #======================================

        def SQL_BLEGATEWAY_INIT(self) :
                try :
                        #global gSqlIndex
                        ret = self.SQL_getCountGateway()
                        print ret
                        if( (ret == 0) or (ret == None) ) :
                                print("no data : Insert : "),
                                self.SQL_AddGatewayInfo()
                                #self.db.commit()
                                print("Data Committed")

                        else :
                                print("Update:"),    #,without carrige return
                                self.SQL_UpdateGatewayInfo()
                                #self.db.commit()
                                print("Data Committed")

                                self.gSqlIndex = self.SQL_getGatewayIndex();
                                print("Gateway Index = %d") % (self.gSqlIndex)
                except :
                        print('SQL_BLEGATEWAY_INIT Error :: Unknown table')



        def SQL_INPUTGATEWAY(self) :
                #CreatedBTable.gGatewayName = 'Anybi:n001'
                #CreatedBTable.gGatewayIP = '192.168.0.15'
                self.gIPAddress = ' 192.168.0.15'
                self.gSCANNER_ID = 'Anybi:n001'
                self.gConstrain = '1st'
                self.gProductco = 'YONWOO'
                self.gLocation = 'YW_CL'
                self.gLocx = '0'
                self.gLocy = '0'
                self.gLocz = '0'
                self.gDatetime = str(datetime.now())

        def checkMOLDLIST(self, DATA) :
                try :

                        for i, s in enumerate(self.data_MoldLoc) :
                                #print (s, i)
                                if DATA.upper() in s[1].upper():
                                        #print i, s
                                        return i, (s[0])

                        return 0
                except :
                        return -1

        def SQL_getNEWMoldLoc(self) :

                del self.data_MoldLoc[:]

                try :
                        query = "SELECT mold_blackbin_name, mold_blackbin_mac FROM mold_loc order by mold_loc_index asc"

                        self.curs.execute(query)
                        for r in self.curs.fetchall() :
                                #print(r)
                                #tt = r.upper()
                                self.data_MoldLoc.append(r)

                        #return (data_MoldLoc)

                except :
                        print('SQL_getNEWMoldLoc Error')

        def SQL_DELETE(self) :
                self.curs.close()

#======================================
#==========================================================
# main function
#==========================================================
def ble_main():

        #a = []

        try :
                print('New anybin 2017 :: V1.0')
                a = CreatedBTable()

                a.SQL_MSSQL_OPEN()

                #a.ShowTables()
                ret1 = a.DROPTABLES()
                print(ret1)

                ret = a.CREATE_TABLES()
                print( len(ret), ret)

                if(ret[0] > 1) :
                        a.SQL_BLEGATEWAY_INIT()

                if(ret[1] >= 0) :
                        # ble_device data insert
                        a.SQL_INPUTDATA()                       #ble_device information input
                        a.SQL_InsertBLE()

        except :
                print ('ble main :: Error')

        finally :
                #a.SQL_DELETE()
                print('ble main normal close')


#======================================


#==========================================================
# main function
#==========================================================
def main():

        while(1) :
                ble_main()
                time.sleep(5)
                break;
        print('dB End')

#==========================================================

if __name__ == '__main__':
        main()

#==========================================================

