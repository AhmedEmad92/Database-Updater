import os
import configparser as ConfigParser
from os import path


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

    SHOST = appConfig.get("SourceConnectionString", "host")
    SUSER = appConfig.get("SourceConnectionString", "user")
    SPASSWORD = appConfig.get("SourceConnectionString", "password")
    SPORT = appConfig.get("SourceConnectionString", "port")
    SDATABASE = appConfig.get("SourceConnectionString", "db")
    SDRIVER = appConfig.get("SourceConnectionString", "driver")
    STABLE = appConfig.get("SourceConnectionString", "table")
    SCOLUMN = appConfig.get("SourceConnectionString", "columns")
    SPKEY = appConfig.get("SourceConnectionString", "pk")
    HAS_OBS = appConfig.get("SourceConnectionString", "has_observers")
    SOBS = appConfig.get("SourceConnectionString", "observers")
    SFILTER = appConfig.get("SourceConnectionString", "filters")
    HAS_FILTER = appConfig.get("SourceConnectionString", "has_filters")
    S_SYNC_TABLE = appConfig.get("SourceConnectionString", "sync_table")
    LIMIT = appConfig.get("SourceConnectionString", "limit")
    DEBUG = appConfig.get("SourceConnectionString", "debug")
    SLEEP = appConfig.get("SourceConnectionString", "sleep(s)")
    HAS_ARABIC = appConfig.get("SourceConnectionString", "PK_HAS_ARABIC")
    SERVPORT = appConfig.get("SourceConnectionString", "serverport")

    DHOST = appConfig.get("DestConnectionString", "host")
    DDATABASE = appConfig.get("DestConnectionString", "db")
    DUSER = appConfig.get("DestConnectionString", "user")
    DPASSWORD = appConfig.get("DestConnectionString", "password")
    DPORT = appConfig.get("DestConnectionString", "port")
    DDRIVER = appConfig.get("DestConnectionString", "driver")
    DTABLE = appConfig.get("DestConnectionString", "table")

    return[SHOST,SUSER,SPASSWORD,SDATABASE,SDRIVER,SPORT,STABLE,SCOLUMN,SPKEY,SOBS,SFILTER,S_SYNC_TABLE,DHOST,DDATABASE,DUSER,DPASSWORD,DPORT,DDRIVER,DTABLE,HAS_FILTER,HAS_OBS,LIMIT,DEBUG,SLEEP,HAS_ARABIC,SERVPORT]


def serv_conn(fname):

    server_creation(fname)
    appConfig = ConfigParser.ConfigParser()
    appConfig.read(fname)
    SERVPORT = appConfig.get("SourceConnectionString", "serverport")

    return SERVPORT
