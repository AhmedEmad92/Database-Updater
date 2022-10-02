from audioop import add
import zlib
import socket    as s         
import threading
from configparser import ParsingError
import pyodbc
import cx_Oracle
from connection import serv_conn
from updater_client import Msg
# import time
# import numpy as np
# import io 
import pickle

HEADER = 64
try:
    PORT = int(serv_conn("server.config"))
except :
    PORT = 5050
SERVER = s.gethostbyname(s.gethostname())
ADDR = (SERVER,PORT)
FORMAT = 'utf-8'
DISCONNECT_MSG = "!DISCONNECT"
server=s.socket(s.AF_INET,s.SOCK_STREAM)
server.bind(ADDR)

def handle_client(s_conn,addr):
    print(f"[NEW_CONNECTION] {addr} connected")
    connected = True
    Er = True
    while connected:
        comp= s_conn.recv(4096)
        if comp:
            decomp= zlib.decompress(comp)
            if decomp:
                msg:Msg = pickle.loads(decomp)
                if msg:
                    sqlst= msg.Sqlst
                    shost= "localhost"
                    sport=msg.SPORT
                    suser=msg.SUSER
                    spass=msg.SPASSWORD
                    sdrive=msg.SDRIVER
                    pkey=msg.PKEY
                    spkey=msg.PKEY.split (",")
                    n_pkey = len(spkey)
                    s=msg.S
                    sdatabase=msg.SDATABASE
                    limit =  msg.LIMIT
                    done = msg.DONE
                    sync=msg.S_SYNC_TABLE
                    col=msg.COL
                    # ,HAS_OBST,OBS
                    has_obs = msg.HAS_OBST
                    obs=msg.OBS
                    sobs=msg.OBS.split (",")
                    scol = msg.SCOLUMN
                    arabic = msg.HAS_ARABIC
                    debug_= msg.DEBUG_
                    nobs=len(sobs)
                    if Er == True :
                        if s=="SQL":
                            comm = pyodbc.connect('DRIVER='+sdrive+';SERVER='+shost+';DATABASE='+sdatabase+';UID='+suser+';PWD='+spass+';Trusted_Connection=no'+';' )
                            cursor = comm.cursor()
                        elif s=="ORACLE":
                            s_tns = cx_Oracle.makedsn(shost, sport, service_name=sdatabase)
                            comm = cx_Oracle.connect(user=suser, password=spass, dsn=s_tns) 
                            cursor = comm.cursor()
                        Er = False
                    if done == "1":
                        # cursor = comm.cursor()
                        if s=="SQL":
                            cursor.fast_executemany = True
                        cursor.execute(sqlst)
                        for ora in cursor:
                            cnt  = ora[0]
                        print("Remain " + str(cnt))
                        message=pickle.dumps(cnt)
                        message=zlib.compress(message)
                        s_conn.send(message)
                    elif done == " ":
                        cursor.execute(sqlst)
                        rows=cursor.fetchmany(limit)
                        if debug_==1 :
                            print(str(rows))
                        message=pickle.dumps(rows)
                        message = zlib.compress(message)
                        print("Sending Data to Client ...")
                        s_conn.send(message)
                    elif done =="done":
                        if limit > cnt :
                            limit = cnt
                        lim=0
                        while lim< limit:
                            sqlst = "SELECT COUNT(*) FROM  " + sync + " WHERE "
                            c=0
                            val=rows[lim]
                            index_of_pk=0
                            while n_pkey != c :
                                index_of_pk=col.index(spkey[c])
                                if s== "SQL" and arabic == "Y":
                                        sqlst = sqlst +sync+"."+ spkey[c] +"=" + "N'" +str(val[index_of_pk]) + "'"
                                else:
                                    sqlst = sqlst +sync+"."+ spkey[c] +"=" + "'" +str(val[index_of_pk]) + "'"
                                c=c+1
                                if c!=n_pkey:
                                    sqlst=sqlst+ " and "
                            if debug_==1 :
                                print(sqlst)
                            # cursor = comm.cursor()
                            if s=="SQL":
                                cursor.fast_executemany = True
                            cursor.execute(sqlst)
                            for ora in cursor:
                                    count_ = ora[0]
                            if count_ == 0 :
                                sqlst  = "INSERT INTO "+sync +"( "+pkey 
                                if has_obs == "Y":
                                    sqlst = sqlst +","+obs+" ) VALUES ("
                                elif has_obs == "N":
                                    sqlst = sqlst + " ) VALUES ("
                                c=0
                                index_of_pk=0
                                while n_pkey != c :
                                    index_of_pk=col.index(spkey[c])
                                    if s == "ORACLE" :
                                        sqlst = sqlst + "'" +str(val[index_of_pk]) +"'"
                                    elif s=="SQL" and arabic == "Y":
                                        sqlst = sqlst + "N'" +str(val[index_of_pk]) +"'"
                                    elif s=="SQL" and arabic == "N":
                                        sqlst = sqlst + "'" +str(val[index_of_pk]) +"'"
                                    c=c+1
                                    if c != n_pkey:
                                        sqlst = sqlst+","
                                    elif has_obs == "N":
                                        sqlst=sqlst + ")"
                                    elif has_obs =="Y":
                                        sqlst=sqlst+","
                                        index_of_obs=0
                                        cc=0
                                        while nobs != cc :
                                            index_of_obs=col.index(sobs[cc])
                                            if val[cc]==None and s =="SQL" :
                                                sqlst = sqlst + "NULL"
                                                cc=cc+1
                                            elif val[cc]==None and s =="ORACLE" :
                                                sqlst = sqlst + "''"
                                                cc=cc+1
                                            elif s == "SQL":
                                                sqlst = sqlst + "N'" +str(val[index_of_obs])  + "'"
                                                cc=cc+1
                                            else:
                                                sqlst = sqlst + "'" +str(val[index_of_obs])  + "'"
                                                cc=cc+1
                                            if cc!= nobs:
                                                sqlst=sqlst+","
                                            else:
                                                sqlst=sqlst+")"
                                if debug_==1 :
                                    print(sqlst)
                                # cursor.execute(sqlst)  
                                # comm.commit()
                            else:
                                sqlst = "UPDATE "+sync +" SET "
                                i=nobs
                                q=0
                                while q != i :
                                    index_of_obs=col.index(sobs[q])
                                    sqlst = sqlst + sobs[q] +"= " 
                                    if val[q]==None and s =="SQL" :
                                        sqlst = sqlst + "NULL"
                                        q=q+1
                                    elif val[q]==None and s =="ORACLE" :
                                        sqlst = sqlst + "''"
                                        q=q+1
                                    elif s == "SQL":
                                        sqlst = sqlst + "N'" +str(val[index_of_obs])  + "'"
                                        q=q+1
                                    else:
                                        sqlst = sqlst + "'" +str(val[index_of_obs])  + "'"
                                        q=q+1
                                    if q!= i:
                                        sqlst=sqlst+","
                                sqlst=sqlst+" WHERE " 
                                nn=0
                                index_of_pk=0
                                while n_pkey != nn :
                                    index_of_pk=col.index(spkey[nn])
                                    if s== "SQL" and arabic == "Y":
                                            sqlst = sqlst +sync+"."+ spkey[nn] +"=" + "N'" +str(val[index_of_pk]) + "'"
                                    else:
                                        sqlst = sqlst +sync+"."+ spkey[nn] +"=" + "'" +str(val[index_of_pk]) + "'"
                                    nn=nn+1
                                    if nn!=n_pkey:
                                        sqlst=sqlst+ " and "
                                if debug_==1 :            
                                    print(sqlst)
                            if s=="SQL":
                                cursor.fast_executemany = True
                            cursor.execute(sqlst)          
                            lim=lim+1
                        comm.commit()
                        print("commited " + str(limit)+ " rows")
                        message=pickle.dumps([0])
                        message = zlib.compress(message)
                        s_conn.send(message)
        else:
            connected = False
            print(f"Disconnected {addr} !")

def start():
    server.listen()
    print("Updater Server v3.2")
    print(f"LISTENING SERVER is listening on PORT {SERVER} : {PORT}")
    while True:
        s_conn,addr = server.accept()
        # handle_client(s_conn,addr)
        thread = threading.Thread(target=handle_client,args=(s_conn,addr))
        thread.start()
        print(f"[ACTIVE_CONNECTIONS] {threading.active_count()-1}")

print("server is starting")
try:
    start()
except:
    print("error in connection ..")
    Er = 1