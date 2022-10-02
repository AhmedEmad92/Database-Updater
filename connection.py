import os
import configparser as ConfigParser
from os import path


class Conf:
    SHOST :str
    SUSER :str
    SPASSWORD :str
    SPORT :str
    SDATABASE:str
    SDRIVER :str
    STABLE :str
    SCOLUMN :str
    SPKEY:str
    HAS_OBS :str
    SOBS :str
    SFILTER :str 
    HAS_FILTER:str
    S_SYNC_TAB:str
    LIMIT :str
    DEBUG :str
    SLEEP :str
    HAS_ARABIC:str
    SERVPORT :str
    S_SYNC_TABLE:str
    DHOST :str
    DDATABASE:str
    DUSER :str
    DPASSWORD :str
    DPORT :str
    DDRIVER :str
    DTABLE :str
    
  

# CREATE CONN FILE IF NOT EXISTS

def client_creation(fname):
    if not path.exists(fname) :
        LIMIT = input("please input the limit for commit : ")
        SERVPORT = input("please input the Server port : ")
        DEBUG = input("turn on or off the debug 0/1 : ")
        SLEEP = input("input sleep time in seconds : ")
        SHOST = input("please input the source server ip: ")
        SDATABASE = input("please input source database name: ")
        SUSER = input("please input source database user name: ")
        SPASSWORD = input("please input source database password: ")
        SPORT = input("please input source database port: ")
        SDRIVER = input("please input the source driver (o=oracle , s=sql server): ")
        STABLE = input("please input the source table: ")
        SCOLUMN = input("please input the source table columns: ")
        SPKEY = input("please input the source table PK: ")
        HAS_OBS = input("does it has OBSERVERS 'y/n': ")
        SOBS =  input("please input the table OBSERVERS: ")
        HAS_FILTER = input("does it has filter 'y/n': ")
        SFILTER = input("please input the filter: ")
        HAS_ARABIC = input("DOES PK HAS ARABIC LETTER? 'y/n': ")
        S_SYNC_TABLE = input("please input the source sync_table: ")
        DHOST = input("please input the DESTINATION server ip: ")
        DDATABASE = input("please input DESTINATION database name: ")
        DUSER = input("please input DESTINATION database user name: ")
        DPASSWORD = input("please input DESTINATION database password: ")
        DPORT = input("please input DESTINATION database port: ")
        DDRIVER = input("please input the DESTINATION driver (o=oracle , s=sql server): ")
        DTABLE = input("please input the DESTINATION table: ")
        configfile_name = fname

        # Check if there is already a configurtion file
        if not os.path.isfile(configfile_name):
            # Create the configuration file as it doesn't exist yet
            cfgfile = open(configfile_name, "w")
            # Add content to the file
            Config = ConfigParser.ConfigParser()
            Config.add_section("SourceConnectionString")
            Config.set("SourceConnectionString", "limit", LIMIT)
            Config.set("SourceConnectionString", "serverport", SERVPORT)
            Config.set("SourceConnectionString", "debug", DEBUG)
            Config.set("SourceConnectionString", "sleep(s)", SLEEP)
            Config.set("SourceConnectionString", "host", SHOST)
            Config.set("SourceConnectionString", "db", SDATABASE)
            Config.set("SourceConnectionString", "user", SUSER)
            Config.set("SourceConnectionString", "password", SPASSWORD)
            Config.set("SourceConnectionString", "port", SPORT)
            Config.set("SourceConnectionString", "driver", SDRIVER)
            Config.set("SourceConnectionString", "table", STABLE)
            Config.set("SourceConnectionString", "columns", SCOLUMN)
            Config.set("SourceConnectionString", "pk", SPKEY)
            Config.set("SourceConnectionString", "has_observers", HAS_OBS)
            Config.set("SourceConnectionString", "observers", SOBS)
            Config.set("SourceConnectionString", "has_filters", HAS_FILTER)
            Config.set("SourceConnectionString", "filters", SFILTER)
            Config.set("SourceConnectionString", "sync_table", S_SYNC_TABLE)
            Config.set("SourceConnectionString", "PK_HAS_ARABIC", HAS_ARABIC)
            Config.add_section("DestConnectionString")
            Config.set("DestConnectionString", "host", DHOST)
            Config.set("DestConnectionString", "db", DDATABASE)
            Config.set("DestConnectionString", "user", DUSER)
            Config.set("DestConnectionString", "password", DPASSWORD)
            Config.set("DestConnectionString", "port", DPORT)
            Config.set("DestConnectionString", "driver", DDRIVER)
            Config.set("DestConnectionString", "table", DTABLE)
            Config.write(cfgfile)
            cfgfile.close()
    # END OF CREATE AND ENCRYPTION :))


def server_creation(fname):
    if not path.exists(fname) :
        SERVPORT = input("please input the Server port : ")
        configfile_name = fname

        # Check if there is already a configurtion file
        if not os.path.isfile(configfile_name):
            # Create the configuration file as it doesn't exist yet
            cfgfile = open(configfile_name, "w")
            # Add content to the file
            Config = ConfigParser.ConfigParser()
            Config.add_section("SourceConnectionString")
            Config.set("SourceConnectionString", "serverport", SERVPORT)
            Config.write(cfgfile)
            cfgfile.close()
    # END OF CREATE AND ENCRYPTION :))

# NOW MAIN FUNCTION WHICH DECRYPT AND ENCRYPT ALL CONN DATA 
def conn(fname):

    client_creation(fname)
    appConfig = ConfigParser.ConfigParser()
    appConfig.read(fname)
    conf= Conf
    conf.SHOST = appConfig.get("SourceConnectionString", "host")
    conf.SUSER = appConfig.get("SourceConnectionString", "user")
    conf.SPASSWORD = appConfig.get("SourceConnectionString", "password")
    conf.SPORT = appConfig.get("SourceConnectionString", "port")
    conf.SDATABASE = appConfig.get("SourceConnectionString", "db")
    conf.SDRIVER = appConfig.get("SourceConnectionString", "driver")
    conf.STABLE = appConfig.get("SourceConnectionString", "table")
    conf.SCOLUMN = appConfig.get("SourceConnectionString", "columns")
    conf.SPKEY = appConfig.get("SourceConnectionString", "pk")
    conf.HAS_OBS = appConfig.get("SourceConnectionString", "has_observers")
    conf.SOBS = appConfig.get("SourceConnectionString", "observers")
    conf.SFILTER = appConfig.get("SourceConnectionString", "filters")
    conf.HAS_FILTER = appConfig.get("SourceConnectionString", "has_filters")
    conf.S_SYNC_TABLE = appConfig.get("SourceConnectionString", "sync_table")
    conf.LIMIT = appConfig.get("SourceConnectionString", "limit")
    conf.DEBUG = appConfig.get("SourceConnectionString", "debug")
    conf.SLEEP = appConfig.get("SourceConnectionString", "sleep(s)")
    conf.HAS_ARABIC = appConfig.get("SourceConnectionString", "PK_HAS_ARABIC")
    conf.SERVPORT = appConfig.get("SourceConnectionString", "serverport")

    conf.DHOST = appConfig.get("DestConnectionString", "host")
    conf.DDATABASE = appConfig.get("DestConnectionString", "db")
    conf.DUSER = appConfig.get("DestConnectionString", "user")
    conf.DPASSWORD = appConfig.get("DestConnectionString", "password")
    conf.DPORT = appConfig.get("DestConnectionString", "port")
    conf.DDRIVER = appConfig.get("DestConnectionString", "driver")
    conf.DTABLE = appConfig.get("DestConnectionString", "table")

    return conf


def serv_conn(fname):

    server_creation(fname)
    appConfig = ConfigParser.ConfigParser()
    appConfig.read(fname)
    SERVPORT = appConfig.get("SourceConnectionString", "serverport")

    return SERVPORT
