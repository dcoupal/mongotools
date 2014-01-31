#!/usr/bin/env python

'''
Created in January 2014

@author: Daniel Coupal

Script to restore an MMS instance.
  - it connects to the MMS host to receive the data
  - explodes the .gzip file
  - clean the data
    - remove the MMS configuration, so we don't overwrite the target
  - restore the data with 'mongorestore' and 'mongoimport'
  - creates an entry about that restore, so we get the a trace of the import, the time, ...
  
Instructions for using the tools are at:
  https://wiki.mongodb.com/display/cs/MMS+Exporter+and+Importer
  
Pre-requisites:
  - Python < 2.3 and > 3.0
  - user must have 'mongo', 'mongoexport' and 'mongodump' in path, or set MONGO_HOME

Implementation details:
  - A lot of the functions are imported from 'mongo_mms_export.py' instead of being shared
    in a common file. The reason was to be able to give a single stand alone file to the
    customers, so it is also used as the library for the common functions.

 TODOs
  - Add a security check, so the customers don't run this tool by mistake and import
    in their own database.
  - add support for an authenticated MMS DB
'''

import bson
import optparse
import os
import pymongo
import re
import shutil
import socket
import sys
import tarfile
import time
import traceback

ROOTDIR = os.path.dirname(__file__)
sys.path.insert(0, ROOTDIR)
import mongo_mms_export

TOOL = "mongo_mms_import"
VERSION = "0.1.0"

DEPS = [ "mongo", "mongoimport", "mongorestore" ]
PID = os.getpid()

COLLECTIONS_TO_IMPORT = [ ("mmsdbconfig", "config.customers"), mongo_mms_export.IMPORTER_LOGS ] # IMPROVE, find all collections by looking at dir, except ("cloudconf", "app.migrations")

Verbose = False

def get_opts():
    '''
    Read the options and arguments provided on the command line.
    '''
    parser = optparse.OptionParser(version="%prog " + VERSION)
    group_general = optparse.OptionGroup(parser, "General options")
    parser.add_option_group(group_general)
    group_general.add_option("-d", "--data", dest="data", type="string", default="", help="name of the .gzip file or directory to import", metavar="FILE")
    group_general.add_option("--host", dest="host", type="string", default='localhost', help="host name of the MMS server", metavar="HOST")
    group_general.add_option("-p", "--port", dest="port", type="string", default='27017', help="port of the MMS server", metavar="PORT")
    group_general.add_option("--password", dest="password", type="string", default='', help="password for a secured MMS DB", metavar="PASSWORD")
    group_general.add_option("-t", "--tmpdir", dest="tmpdir", type="string", default=".", help="temporary dir to use for the restore", metavar="DIR")
    group_general.add_option("-u", "--upsert", dest="upsert", action="store_true", default=False, help="upsert/update the data that already exists")
    group_general.add_option("--username", dest="username", type="string", default='', help="username for a secured MMS DB", metavar="USERNAME")
    group_general.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="show more output")
    group_security = optparse.OptionGroup(parser, "Security options")
    parser.add_option_group(group_security)
    group_security.add_option("-n", "--norun", dest="norun", action="store_true", default=False, help="don't run, just show what would be run")
    (options, args) = parser.parse_args()
    return options, args

def add_data(directory, groups):
    col_filepath = os.path.join(directory, mongo_mms_export.COLLECTIONS_DIR, mongo_mms_export.IMPORTER_LOGS[0], mongo_mms_export.IMPORTER_LOGS[1])
    now = int(round(time.time() * 1000))
    groups_string = "["
    for one_group in groups:
        groups_string += '"%s",' % (one_group)
    if groups_string.endswith(","):
        groups_string = groups_string[:-1]
    groups_string += "]"
    add_fields = ', "import_host":"%s", "import_ts":{"$date":%d}, "groups":%s' % (socket.gethostname(), now, groups_string)
    mongo_mms_export.replace_string(col_filepath, "}\n", add_fields + "}\n")
    
def clean_data(directory):
    '''
    Remove the MMS config data
    :param directory: root dir from which we do the cleaning
    '''
    col_dir = os.path.join(directory, mongo_mms_export.DB_CLOUDCONF)
    if os.path.exists(col_dir):
        shutil.rmtree(col_dir)

def explode_gzip(gzipfile, target_dir):
    '''
    Explode the gzip file to a target directory
    :param gzipfile: file to explode
    :param target_dir: target location for the files
    '''
    print "Exploding gzip file...",
    tar = tarfile.open(gzipfile, "r:gz")
    tar.extractall(path=target_dir)    
    tar.close()
    print " done."    
    
def get_data_mms_version(directory):
    '''
    Get the MMS version of the data to import
    :param directory: directory of the data to import
    '''
    version = None
    version_path = os.path.join(directory, mongo_mms_export.MMS_VERSION_FILE)
    if os.path.isfile(version_path):
        version_file = open(version_path, 'r')
        version = version_file.read().strip()
        version_file.close()
    return version
        
def get_mms_version(auth_dict, host, port):
    '''
    Get the MMS version of the target instance.
    :param host: of the target MMS instance.
    :param port: of the target MMS instance.
    '''
    version = None
    int_port = int(port)
    client = pymongo.mongo_client.MongoClient(host=host, port=int_port)
    if auth_dict is not None:
        client['admin'].authenticate(auth_dict['username'], auth_dict['password'], source=auth_dict['auth_database'])
    # Ensure all aggregations, alerts, ... settings are turned off
    coll = 'app.migrations'
    if Verbose:
        print "Counting documents in DB:%s COLL:%s" % (mongo_mms_export.DB_CLOUDCONF, coll)
    db = client[mongo_mms_export.DB_CLOUDCONF]
    coll = db[coll]
    count = coll.count()
    if count == 1:
        version = "1.2"
    elif count == 11:
        version = "1.3"
    elif count > 11:
        # For versions we don't know...
        version = str(count)
    else:
        version = "1.1"
    return version

def restore_database(mongorestore, mongoimport, auth_string, host, port, directory, upsert):
    '''
    Load the MMS data into our target instance.
    :param host: of the target MMS instance
    :param port: of the target MMS instance
    :param directory: root dir of the data to import
    :param mongorestore: path to mongorestore
    :param mongoimport: path to mongoimport
    :param upsert: upsert/overwrite existing data
    '''
    print "Restoring database"
    print "  First, the 'dump' part..."
    cmd = "%s %s --host %s --port %s --verbose" % (mongorestore, auth_string, host, port)
    cmd = "cd %s && %s" % (directory, cmd)
    mongo_mms_export.run_cmd(cmd, abort=True)
    print "  Secondly, the exported collections..."
    for db_coll in COLLECTIONS_TO_IMPORT:
        (db, coll) = db_coll
        json_file = os.path.join(directory, mongo_mms_export.DUMPDIR, mongo_mms_export.COLLECTIONS_DIR, db, coll)
        cmd = "%s %s --host %s --port %s -d %s -c %s --file %s" % (mongoimport, auth_string, host, port, db, coll, json_file)
        if upsert:
            cmd = cmd + " --upsert"
        mongo_mms_export.run_cmd(cmd, abort=True)
    # show groups being restored
    print "  done."
  
def set_defaults(auth_dict, host, port, mms_version):
    '''
    Set/reset some default values and settings in the target database.
    For example:
      - ensure all 'mongodb.com' users have access to all DBs.
    :param host: of the target MMS instance
    :param port: of the target MMS instance
    :param mms_version: of the target instance
    '''
    if Verbose:
        print "Setting/resetting default values on MMS viewer instance"
    int_port = int(port)
    client = pymongo.mongo_client.MongoClient(host=host, port=int_port)
    if auth_dict is not None:
        client['admin'].authenticate(auth_dict['username'], auth_dict['password'], source=auth_dict['auth_database'])
    # Ensure all aggregations, alerts, ... settings are turned off
    coll = 'app.systemCronState'
    if Verbose:
        print "  Modifying DB:%s COLL:%s" % (mongo_mms_export.DB_CLOUDCONF, coll)
    db = client[mongo_mms_export.DB_CLOUDCONF]
    coll = db[coll]
    coll.update({},{"$set":{"enabled":False}}, upsert=False, multi=True)
    # Ensure specific alerts are turned off
    coll = 'config.alertSettings'
    if Verbose:
        print "  Modifying DB:%s COLL:%s" % (mongo_mms_export.DB_MMSCONF, coll)
    db = client[mongo_mms_export.DB_MMSCONF]
    coll = db[coll]
    coll.update({},{"$set":{"enabled":False}}, upsert=False, multi=True)
    # All our internal users should have access to all groups
    coll = 'config.users'
    if Verbose:
        print "  Modifying DB:%s COLL:%s" % (mongo_mms_export.DB_MMSCONF, coll)
    db = client[mongo_mms_export.DB_MMSCONF]
    coll = db[coll]
    if mms_version >= "1.3":
        coll.update({"pe":{"$regex":"mongodb.com"}}, {"$addToSet":{"roles": {"role":"XGEN_USER"}}}, upsert=False, multi=True)
    elif mms_version == "1.2":
        # Magic to make the users see all groups
        oid = bson.objectid.ObjectId(oid="4d09359b1cc223ebd7f9797f")
        coll.update({"pe":{"$regex":"mongodb.com"}}, {"$addToSet": {"cids":oid}, "$set":{"xe":True}}, upsert=False, multi=True)

def show_imported_groups(extract_dir):
    groups = []
    coll_filepath = os.path.join(extract_dir, mongo_mms_export.DUMPDIR, mongo_mms_export.COLLECTIONS_DIR, mongo_mms_export.COLLECTION_WITH_GROUPS[0], mongo_mms_export.COLLECTION_WITH_GROUPS[1])
    coll_file = open(coll_filepath)
    for line in coll_file.readlines():
        m = re.search(r'"n"\s*:\s*"(.+?)"', line)
        if m:
            groups.append(m.group(1))
    print "Groups imported: %s" % (groups,)
    return sorted(groups)

def main():
    '''
    The main module.
    '''
    global Verbose
    sys.stdout = mongo_mms_export.flushfile(sys.stdout)
    (options, args) = get_opts()
    if args:
        mongo_mms_export.fatal("Found trailing arguments: %s" % (str(args)))
    if options.verbose:
        Verbose = True
        mongo_mms_export.Verbose = True
        print "Verbose mode on, will show more info..."
        print "%s version %s" % (TOOL, VERSION)
        print "Running Python version %s" % (sys.version)
    auth_string = ''
    auth_dict = None
    if options.username or options.password:
        if not options.username or not options.password:
            mongo_mms_export.fatal("You must provide both: --username and --password")
        else:
            auth_string = "--username %s --password %s --authenticationDatabase %s" % (options.username, options.password, mongo_mms_export.AUTH_DB)
            auth_dict = dict()
            auth_dict['username'] = options.username
            auth_dict['password'] = options.password
            auth_dict['auth_database'] = mongo_mms_export.AUTH_DB
    try:
        options.host = mongo_mms_export.get_host(options.host)
        paths = mongo_mms_export.find_paths(DEPS)
        mms_version = get_mms_version(auth_dict, options.host, options.port)
        if options.data:
            need_rm_extract_dir = False
            if not os.path.exists(options.data):
                mongo_mms_export.fatal("Can't find gzip file or directory to import: %s" % (options.data))
            if os.path.isfile(options.data):
                extract_dir = os.path.join(options.tmpdir, str(PID))
                #need_rm_extract_dir = True
                if os.path.exists(extract_dir):
                    mongo_mms_export.warning("Remove previously left over temp dir: %s" % (extract_dir))
                    shutil.rmtree(extract_dir)
                explode_gzip(options.data, extract_dir)
            elif os.path.isdir(options.data):
                # Assume the format and contents is already right
                extract_dir = options.data
            dump_dir = os.path.join(extract_dir, mongo_mms_export.DUMPDIR)
            if not os.path.exists(dump_dir):
                mongo_mms_export.fatal("Can't find the dump directory to restore: %s" % (dump_dir))
            data_mms_version = get_data_mms_version(dump_dir)
            if data_mms_version != mms_version:
                mongo_mms_export.fatal("Can't import MMS data in version %s into a MMS server version %s" % (data_mms_version, mms_version))
            groups = show_imported_groups(extract_dir)
            clean_data(dump_dir)
            add_data(dump_dir, groups)
            restore_database(paths['mongorestore'], paths['mongoimport'], auth_string, options.host, options.port, extract_dir, options.upsert)
            # Clean the dump tree
            if need_rm_extract_dir:
                if Verbose:
                    print "Removing temp dump directory"
                shutil.rmtree(extract_dir)
        set_defaults(auth_dict, options.host, options.port, mms_version)
            
    except Exception, e:
        mongo_mms_export.error("caught exception:\n  " + e.__str__())
        if Verbose:
            traceback.print_exc()
    if mongo_mms_export.Errors:
        print "The script terminated with errors"
    
         
if __name__ == '__main__':
    main()


