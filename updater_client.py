# import threading
import socket    as s       
# from configparser import ParsingError
import pyodbc
from connection import conn
import cx_Oracle
import time
import pickle
import zlib

print("Database Updater V3.13")      
# import sal  
# def send():
# def updater_conn() :
conf = conn("copy.config")
# [SHOST,SUSER,SPASSWORD,SDATABASE,SDRIVER,
#  SPORT,STABLE,SCOLUMN,SPKEY,SOBS,SFILTER,
#  S_SYNC_TABLE,DHOST,DDATABASE,DUSER,
#  DPASSWORD,DPORT,DDRIVER,DTABLE,HAS_FILTER,
#  HAS_OBS,LIMIT,DEBUG,SLEEP,DELETE_O]
SHOST = conf[0]
SHOST2 = conf[0]
SUSER = conf[1]
SPASSWORD = conf[2]
SDATABASE = conf[3]
if conf[4].capitalize() =='S' :
    SDRIVER = 'SQL Server Native Client 11.0'
    COMMIT_DRIVER = 'SQL Server'
    S ="SQL"
    SHOST = str(SHOST) + ","+str(conf[5])
elif conf[4].capitalize() =='O':
    SDRIVER = 'ORACLE'
    S = "ORACLE"
SPORT = conf[5]
STABLE=conf[6]
SCOLUMN=conf[7]
COL = conf[7].split(",")
SPKEY= conf[8].split (",")
PKEY= conf[8]
SOBS=conf[9].split (",")
OBS = conf[9]
if conf[19].capitalize() =='Y' :
    SFILTER=conf[10]
elif conf[19].capitalize() =='N' :
    SFILTER="1=1"
else:
    raise Exception("ERROR IN has_filter")

S_SYNC_TABLE=conf[11]

DHOST=conf[12]
DDATABASE=conf[13]
DUSER=conf[14]
DPASSWORD=conf[15]
DPORT=conf[16]
SCONN = conf[4].capitalize()
DCONN = conf[17].capitalize()
if conf[17].capitalize() =='S' :
    DDRIVER = 'SQL Server Native Client 11.0'
    D = "SQL"
    DHOST = str(DHOST) + ","+str(DPORT)
elif conf[17].capitalize() =='O':
    DDRIVER = 'ORACLE'
    D = "ORACLE"
DTABLE=conf[18]
HAS_OBST = conf[20].capitalize()
LIMIT = int(conf[21])
DEBUG_ = int(conf[22])
N_PKEY = len(SPKEY)
N_OBS = len(SOBS)
SLEEP_ = int(conf[23])
HAS_ARABIC = conf[24].capitalize()
SERVPORT = int(conf[25])

print("starting connection with "+ str(SHOST2) + " on port " + str(SERVPORT))

HEADER = 64
PORT = SERVPORT
FORMAT = 'utf-8'
DISCONNECT_MSG = "!DISCONNECT"
SERVER = SHOST2
# SERVER = '192.168.1.2'
ADDR = (SERVER,PORT)

client =s.socket(s.AF_INET,s.SOCK_STREAM)


def send(msg):
    message=pickle.dumps(msg)
    message=zlib.compress(message)
    client.send(message)
    rows=client.recv(9999999999)
    rows = zlib.decompress(rows)
    rows=pickle.loads(rows)
    return (rows)


def updater():
       
    #    limit = int(input("please type the insert limit " ))
        limit = LIMIT
        ins_error = 0
        if conf[4].capitalize() =='S' :
            conn = pyodbc.connect('DRIVER='+SDRIVER+';SERVER='+SHOST+';DATABASE='+SDATABASE+';UID='+SUSER+';PWD='+SPASSWORD+';Trusted_Connection=no'+';') 
            comm = pyodbc.connect('DRIVER='+COMMIT_DRIVER+';SERVER='+SHOST+';DATABASE='+SDATABASE+';UID='+SUSER+';PWD='+SPASSWORD+';Trusted_Connection=no'+';' )
            # engine = sal.create_engine("mssql+pyodbc://"+SUSER+":"+SPASSWORD+"@"+SHOST+":"+SPORT+"/"+SDATABASE+"?driver=ODBC+Driver+17+for+SQL+Server?Trusted_Connection=yes", fast_executemany = True)
            # comm = engine.connect()
        elif conf[4].capitalize() =='O' :
            s_tns = cx_Oracle.makedsn(SHOST, SPORT, service_name=SDATABASE)
            conn = cx_Oracle.connect(user=SUSER, password=SPASSWORD, dsn=s_tns) 
            comm = conn
        if conf[17].capitalize() =='O':
            d_tns = cx_Oracle.makedsn(DHOST, DPORT, service_name=DDATABASE)
            cnxn = cx_Oracle.connect(user=DUSER, password=DPASSWORD, dsn=d_tns) 
        elif conf[17].capitalize() =='S' :
            cnxn = pyodbc.connect('DRIVER='+DDRIVER+';SERVER='+DHOST+';DATABASE='+DDATABASE+';UID='+DUSER+';PWD='+DPASSWORD+';Trusted_Connection=no'+';') 

        if S=="ORACLE":
            sqlst = "SELECT COUNT(*) FROM ALL_TABLES WHERE TABLE_NAME = "+"'" + S_SYNC_TABLE+"'"
            if DEBUG_==1:
                    print(sqlst)
            cursor = conn.cursor()
            cursor.execute(sqlst)
            for row in cursor:
                found = row[0]
            if found == 0 :
                sqlst="CREATE TABLE " + S_SYNC_TABLE + " AS SELECT " + PKEY
                if HAS_OBST == "Y":
                    sqlst=sqlst+","+OBS 
                sqlst=sqlst+" FROM " + STABLE +" WHERE 1=2"
                if DEBUG_==1:
                    print(sqlst)
                cursor = conn.cursor()
                try:
                    cursor.execute(sqlst)
                except cx_Oracle.DatabaseError as e:
                        error, = e.args
                        print(error.message)
                conn.commit()
                sqlst= "ALTER TABLE "+S_SYNC_TABLE + " add constraint PK_"+S_SYNC_TABLE+" primary key ("+PKEY+")"
                if DEBUG_==1:
                    print(sqlst)
                cursor = conn.cursor()
                try:
                    cursor.execute(sqlst)
                except cx_Oracle.DatabaseError as e:
                        error, = e.args
                        print(error.message)
                conn.commit()
        elif S=="SQL":
            sqlst = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = "+"'"+S_SYNC_TABLE+"'"
            if DEBUG_==1:
                    print(sqlst)
            cursor = conn.cursor()
            cursor.execute(sqlst)
            for row in cursor:
                found = row[0]
            if found == 0 :
                sqlst="SELECT " + PKEY 
                if HAS_OBST == "Y":
                    sqlst=sqlst+","+OBS 
                sqlst=sqlst+" INTO " +S_SYNC_TABLE+" FROM " + STABLE +" WHERE 1=2"
                if DEBUG_==1:
                    print(sqlst)
                cursor = conn.cursor()
                cursor.execute(sqlst)
                conn.commit()
                sqlst = "ALTER TABLE "+S_SYNC_TABLE+" ADD  CONSTRAINT [PK_"+S_SYNC_TABLE+"]"+" PRIMARY KEY CLUSTERED ( "+PKEY+" )"
            if DEBUG_==1:
                    print(sqlst)
            cursor = conn.cursor()
            cursor.execute(sqlst)
            conn.commit()
###################################################################################################################################
        while True:
            sqlst = "SELECT COUNT(*) FROM "+STABLE + " WHERE "+ SFILTER + " AND NOT EXISTS (SELECT 1 FROM "+ S_SYNC_TABLE + " WHERE "
            c=0
            cnt =0
            while N_PKEY != c :
                sqlst = sqlst +STABLE+"."+ SPKEY[c] +"="+S_SYNC_TABLE+"."+ SPKEY[c]
                c=c+1
                if c!=N_PKEY :
                    sqlst=sqlst+ " and "
                elif HAS_OBST =="N" :
                    sqlst=sqlst+ ")"
                elif HAS_OBST == "Y" :
                    sqlst = sqlst +" and "
                    while N_OBS != cnt:
                        sqlst = sqlst +STABLE+"."+ SOBS[cnt] +"="+S_SYNC_TABLE+"."+ SOBS[cnt]
                        cnt = cnt+1
                        if cnt!=N_OBS :
                            sqlst=sqlst+ " and "
                        else:
                                sqlst=sqlst+ ")"
            if DEBUG_ ==1:
                print (sqlst)
            row=send([sqlst,SHOST,SPORT,SUSER,SPASSWORD,SDRIVER,PKEY,S,SDATABASE,LIMIT,"1",S_SYNC_TABLE,COL,HAS_OBST,OBS,SCOLUMN,HAS_ARABIC,DEBUG_])
            # decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            # content = decoder.decompress(row.raw.read())
            # print(row)
            if DEBUG_ ==1:
                 print (sqlst)
            count_ = row
            print(" remain "+ str(count_) + " changes")
            reminder = count_
            if count_ == 0:
                if SLEEP_ >= 3600 :
                    st = SLEEP_/3600
                    st = str(st) + " hours"
                elif SLEEP_ > 60 :
                    st = SLEEP_ /60
                    st = str(st) + " min"
                print("waiting for " + st)
                time.sleep(SLEEP_)
            if count_ > 0 :
                    sqlst = "SELECT "+ SCOLUMN +" FROM "+STABLE + " WHERE "+ SFILTER + " AND NOT EXISTS (SELECT 1 FROM "+ S_SYNC_TABLE + " WHERE "
                    c=0
                    cnt =0
                    while N_PKEY != c :
                        sqlst = sqlst +STABLE+"."+ SPKEY[c] +"="+S_SYNC_TABLE+"."+ SPKEY[c]
                        c=c+1
                        if c!=N_PKEY :
                            sqlst=sqlst+ " and "
                        elif HAS_OBST =="N" :
                            sqlst=sqlst+ ")"
                        elif HAS_OBST == "Y" :
                            sqlst=sqlst+ " and "
                            while N_OBS != cnt:
                                sqlst = sqlst +STABLE+"."+ SOBS[cnt] +"="+S_SYNC_TABLE+"."+ SOBS[cnt]
                                cnt = cnt+1
                                if cnt!=N_OBS :
                                    sqlst=sqlst+ " and "
                                else:
                                        sqlst=sqlst+ ")"
                    if DEBUG_ ==1 :            
                        print (sqlst)
                    rows=send([sqlst,SHOST,SPORT,SUSER,SPASSWORD,SDRIVER,PKEY,S,SDATABASE,LIMIT," ",S_SYNC_TABLE,COL,HAS_OBST,OBS,SCOLUMN,HAS_ARABIC,DEBUG_])
                    # cursor = conn.cursor()
                    # cursor.execute(sqlst)
                    no_row_done = limit
                    cnt=0
                    # rows = cursor.fetchmany(LIMIT)
                    if DEBUG_==1:
                        print(rows)
                    for row in rows:
                        cnt = cnt+1
                        val = row
                        # IF NOT EXISTS
                        sqlst = "SELECT COUNT(*) FROM  " + DTABLE + " WHERE "
                        c=0
                        index_of_pk=0
                        while N_PKEY != c :
                            index_of_pk=COL.index(SPKEY[c])
                            if D== "SQL":
                                if HAS_ARABIC == "Y" :
                                    sqlst = sqlst +DTABLE+"."+ SPKEY[c] +"=" + "N'" +str(val[index_of_pk]) + "'"
                                else:
                                    sqlst = sqlst +DTABLE+"."+ SPKEY[c] +"=" +"'"+str(val[index_of_pk]) + "'"
                            else:
                                sqlst = sqlst +DTABLE+"."+ SPKEY[c] +"=" + "'" +str(val[index_of_pk]) + "'"
                            c=c+1
                            if c!=N_PKEY :
                                sqlst=sqlst+ " and "
                        # print(sqlst)
                        ca = cnxn.cursor()
                        try:
                            if S=="SQL":
                                ca.fast_executemany = True
                            ca.execute(sqlst)
                        except cx_Oracle.DatabaseError as e:
                            error, = e.args
                            print(error.message)
                        for ora in ca:
                            upd_insert = ora[0]
                        if upd_insert == 0 :
                            no_of_val = len(val)
                            l=0
                            sqlst  = "INSERT INTO "+DTABLE +"( "+SCOLUMN +" ) VALUES ("
                            while l != no_of_val:
                                if val[l]==None and D=="ORACLE" :
                                    sqlst  = sqlst+ "''"
                                elif val[l]==None and D=="SQL" :
                                    sqlst  = sqlst +"NULL"
                                elif D== "SQL":
                                    sqlst  = sqlst+"N'" +str(val[l]) + "'" 
                                else:
                                    sqlst  = sqlst+"'" +str(val[l]) + "'" 
                                l=l+1
                                if l!=no_of_val:
                                    sqlst=sqlst+ ","
                                else:
                                    sqlst = sqlst+")" 
                        elif upd_insert ==1:
                            no_of_val = len(val)
                            l=0
                            sqlst  = "UPDATE "+DTABLE +" SET "
                            while l != no_of_val:
                                if val[l]==None and D=="ORACLE"  :
                                    sqlst  = sqlst+COL[l]+"="+"''"
                                elif val[l]==None and D=="SQL"  :
                                    sqlst  = sqlst+COL[l]+"=" "NULL"
                                elif D== "SQL":
                                    sqlst  = sqlst+COL[l]+"= N'" +str(val[l]) + "'" 
                                else:
                                    sqlst  = sqlst+COL[l]+"='" +str(val[l]) + "'" 
                                l=l+1
                                if l!=no_of_val:
                                    sqlst=sqlst+ ","
                                else:
                                    sqlst = sqlst+" WHERE " 
                                    c=0
                                    index_of_pk=0
                                    while N_PKEY != c :
                                        index_of_pk=COL.index(SPKEY[c])
                                        if D== "ORACLE" : 
                                            sqlst = sqlst +DTABLE+"."+ SPKEY[c] +"=" + "'" +str(val[index_of_pk]) + "'"
                                        elif D == "SQL":
                                            sqlst = sqlst +DTABLE+"."+ SPKEY[c] +"=" + "N'" +str(val[index_of_pk]) + "'"
                                        c=c+1
                                        if c!=N_PKEY :
                                            sqlst=sqlst+ " and "
                        if DEBUG_ == 1:
                            print(sqlst)
                        ca = cnxn.cursor()
                        try:
                            if S=="SQL":
                                ca.fast_executemany = True
                            ca.execute(sqlst)
                        except cx_Oracle.DatabaseError as e:
                                error, = e.args
                                print(error.message)
                                ins_error=1
                        if ins_error==0:
                                cnxn.commit()
                                no_of_val = len(val)
                        if cnt == LIMIT or cnt == count_:
                                print("done" + str(cnt))
                                if ins_error ==0 :
                                        cnxn.commit()
                                        l=0
                                        sqlst  = "INSERT INTO "+S_SYNC_TABLE +"( "+PKEY 
                                        if HAS_OBST == "Y":
                                            sqlst = sqlst +","+OBS+" ) VALUES ("
                                        elif HAS_OBST == "N":
                                            sqlst = sqlst + " ) VALUES "
                                    
                                        send([sqlst,SHOST,SPORT,SUSER,SPASSWORD,SDRIVER,PKEY,S,SDATABASE,LIMIT,"done",S_SYNC_TABLE,COL,HAS_OBST,OBS,SCOLUMN,HAS_ARABIC,DEBUG_])
                                        no_row_done=no_row_done+limit
        return 


while True:
    try:
        client =s.socket(s.AF_INET,s.SOCK_STREAM)
        client.connect(ADDR)
    except:
        print("ERROR IN CONNECTION !! CHECK THAT SERVER IS RUNNING !!")
    try:
        updater()
    except:
        print("ERROR RUNNING THE APP PLEASE CHECK THE CONNECTION AND CONFIG FILE")
        time.sleep(60)
