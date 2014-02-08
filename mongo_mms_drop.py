#!/usr/bin/python

'''
Created in January 2014

@author: Daniel Coupal

Script to delete the database of an MMS instance.
  
Instructions for using the tools are at:
  https://wiki.mongodb.com/display/cs/MMS+Exporter+and+Importer
  
Pre-requisites:
  - Python < 2.3 and > 3.0

Implementation details:
  - The MMS service should be stopped prior to running this script

 TODOs
  - do the stop/start of the MMS instance. The annoyance is that you need root privileges..
'''

import optparse
import os
import pymongo
import re
import sys
import traceback

ROOTDIR = os.path.dirname(__file__)
sys.path.insert(0, ROOTDIR)
import mongommsexport

TOOL = "mongommsdrop"
VERSION = "0.1.0"

Verbose = False

def get_opts():
    '''
    Read the options and arguments provided on the command line.
    '''
    parser = optparse.OptionParser(version="%prog " + VERSION)
    group_general = optparse.OptionGroup(parser, "General options")
    parser.add_option_group(group_general)
    group_general.add_option("--host", dest="host", type="string", default='localhost', help="host name of the MMS server", metavar="HOST")
    group_general.add_option("-p", "--port", dest="port", type="string", default='27017', help="port of the MMS server", metavar="PORT")
    group_general.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="show more output")
    group_security = optparse.OptionGroup(parser, "Security options")
    parser.add_option_group(group_security)
    group_security.add_option("--password", dest="password", type="string", default='', help="password for a secured MMS DB", metavar="PASSWORD")
    group_security.add_option("--username", dest="username", type="string", default='', help="username for a secured MMS DB", metavar="USERNAME")
    (options, args) = parser.parse_args()
    return options, args

def drop_databases(auth_dict, host, port):
    if Verbose:
        print "Dropping MMS databases"
    int_port = int(port)
    client = pymongo.mongo_client.MongoClient(host=host, port=int_port)
    if auth_dict is not None:
        client['admin'].authenticate(auth_dict['username'], auth_dict['password'], source=auth_dict['auth_database'])
    for one_db in client.database_names():
        for ok_db in mongommsexport.ALL_MMS_DBS:
            if re.search(ok_db, one_db):
                client.drop_database(one_db)

    
def main():
    '''
    The main module.
    '''
    global Verbose
    sys.stdout = mongommsexport.flushfile(sys.stdout)
    (options, args) = get_opts()
    if args:
        mongommsexport.fatal("Found trailing arguments: %s" % (str(args)))
    if options.verbose:
        Verbose = True
        mongommsexport.Verbose = True
        print "Verbose mode on, will show more info..."
        print "%s version %s" % (TOOL, VERSION)
        print "Running Python version %s" % (sys.version)
    auth_string = ''
    auth_dict = None
    if options.username or options.password:
        if not options.username or not options.password:
            mongommsexport.fatal("You must provide both: --username and --password")
        else:
            auth_string = "--username %s --password %s --authenticationDatabase %s" % (options.username, options.password, mongommsexport.AUTH_DB)
            auth_dict = dict()
            auth_dict['username'] = options.username
            auth_dict['password'] = options.password
            auth_dict['auth_database'] = mongommsexport.AUTH_DB
    try:
        options.host = mongommsexport.get_host(options.host)
        drop_databases(auth_dict, options.host, options.port) 
    except Exception, e:
        mongommsexport.error("caught exception:\n  " + e.__str__())
        if Verbose:
            traceback.print_exc()
    if mongommsexport.Errors:
        print "The script terminated with errors"
    
         
if __name__ == '__main__':
    main()

