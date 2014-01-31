#!/usr/bin/env python

'''
Created in January 2014

@author: Daniel Coupal

Script to export an MMS instance.
  - it connects to the MMS host
  - calculates the size of the data to dump and ensure we have enough space
  - dump all data with 'mongodump' and 'mongoexport'
  - parse the data to remove some potential sensitive data
  - tar the resulting file
  - scp the resulting file in the MongoDB dropbox

Pre-requisites:
  - Python < 2.3 and > 3.0
  - user must have 'mongo' and 'mongodump' in path, or set MONGO_HOME

Implementation details:
  - The script should not 'rm' anything, instead it will tell the user to
    remove things in the way
  - we try to avoid using 'pymongo', prefering doing things in little more
    complicated ways to not add this dependency on customers. We know they
    have 'pymongo' installed somewhere in order to run the MMS agent, 
    but this script may not be running on that machine. Also the current MMS
    agent in Python will be replaced by a Go version.
    
 TODOs
  - because we are running 'mongodump', we don't have control on the DBs we
    are exporting. If users have more than MMS in the DB, we may export way 
    too much. Then we would need to export with 'mongoexport'
  - add mongod credentials
  - support Kerberos
'''
    
import commands
import fileinput
import glob
import optparse
import os
import re
import shutil
import socket
import sys
import tarfile
import time
import traceback

TOOL = "mongo_mms_export"
VERSION = "0.1.0"

AUTH_DB = "admin"
COLLECTIONS_DIR = "_collections"
DB_CLOUDCONF = "cloudconf"
DB_MMSCONF = "mmsdbconfig"
DEPS = ("mongo", "mongodump", "mongoexport")
DUMPDIR = "dump"
FTP_PREFIX = "MMS-"
IMPORTER_LOGS = ("importer", "logs")
MIN_DISK_SPACE = 3000
MMS_VERSION_FILE = "mms_version"
NUL_DOMAIN = "example.com"

FILES_TO_REMOVE = [
                   "cloudconf/app.migrations.bson",
                   "mmsdb/data.emails.bson",
                   "mmsdbconfig/config.alertSettings.bson",
                   "mmsdbconfig/config.customers.bson",
                   "mmsdbconfig/config.users.bson",
                   "mmsdblogs-*/*"
                   ]
COLLECTIONS_TO_EXPORT = [ ("cloudconf", "app.migrations"), ("mmsdbconfig", "config.customers")  ]
COLLECTION_WITH_GROUPS = ("mmsdbconfig", "config.customers")

MAX_UNEXPECTED_DBS = 0
MIN_EXPECTED_DBS = 14
ALL_MMS_DBS = [ r"^apiv3$", r"^alerts$", r"^cloudconf$", r"^importer", r"^mmsdb.*", r"^mongo-distributed-lock$" ]
IGNORE_DBS = [ r"^admin", r"^config$", r"^local$", r"^test$" ]

# OS - specific?
HOSTS_FILE = "/etc/hosts"

Errors = 0
Norun = False
Verbose = False

def get_opts():
    '''
    Read the options and arguments provided on the command line.
    '''
    parser = optparse.OptionParser(version="%prog " + VERSION)
    group_general = optparse.OptionGroup(parser, "General options")
    parser.add_option_group(group_general)
    group_general.add_option("-c", "--caseid", dest="caseid", type="string", default="", help="caseid/ticket to associate the data with, for example 12345 for the case ID ec-12345", metavar="CASEID")    
    group_general.add_option("-d", "--directory", dest="directory", type="string", default=".", help="directory where to put the tar file", metavar="DIR")
    group_general.add_option("-f", "--force", dest="force", action="store_true", default=False, help="force removal of a previous 'dump' directory")
    group_general.add_option("--host", dest="host", type="string", default='localhost', help="host name of the MMS server", metavar="HOST")
    group_general.add_option("-p", "--port", dest="port", type="string", default='27017', help="port of the MMS server", metavar="PORT")
    group_general.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="show more output")
    group_security = optparse.OptionGroup(parser, "Security options")
    parser.add_option_group(group_security)
    group_security.add_option("--nocheck", dest="nocheck", action="store_true", default=False, help="don't run any check, you must ensure you have enough space, ...")
    group_security.add_option("--norun", dest="norun", action="store_true", default=False, help="don't run any command, just show them")
    group_security.add_option("--password", dest="password", type="string", default='', help="password for a secured MMS DB", metavar="PASSWORD")
    group_security.add_option("-s", "--ship", dest="ship", action="store_true", default=False, help="ship the data under the given '-caseid' number")
    group_security.add_option("--username", dest="username", type="string", default='', help="username for a secured MMS DB", metavar="USERNAME")
    group_security.add_option("-z", "--zip", dest="zip", action="store_true", default=False, help="zip the data, but do not ship it")
    (options, args) = parser.parse_args()
    return options, args

def clean_dumped_data(directory):
    '''
    Remove some directories and files from the "dump" directory.
    Those paths are mostly info the customer does not want to send, or data we
    don't want to load in the target MMS instance. For example data like
    settings could overwrite the data in the instance. In general any
    data that is shared by many MMS instances should not be loaded.
    :param directory: where the "dump" dir is located
    '''
    print "Removing sensitive information like user, emails, ..."
    for one_glob in FILES_TO_REMOVE:
        if not Norun:
            for file_to_rm in glob.glob(os.path.join(directory, one_glob)):
                if not os.path.exists(file_to_rm):
                    fatal("That does not look like MMS database, missing collection: %s" % (file_to_rm))
                os.remove(file_to_rm)

def doc_to_json(doc):
    '''
    Return a JSON string from a document.
    The values are either string, or string representations of the types, this is
    not a very intelligent function, it is just to avoid importing 'json' which
    does not exists in Python 2.4
    :param doc: string to transform in JSON
    '''
    doc_str = '{'
    for key in doc.keys():
        doc_str += ' "%s":%s,' % (key, doc[key])
    if doc_str.endswith(','):
        doc_str = doc_str[:-1]
    doc_str += ' }'
    return doc_str

def dump_database(mongodump, auth_string, host, port, directory):
    '''
    Dump the database with "mongodump".
    :param mongodump: path to the executable mongodump.
    :param host: host where the source MMS instance is. Default to localhost.
    :param port: port to access the database. Default to 27017.
    :param directory: directory where to dump to database.
    '''
    print "Dumping database...",
    cmd = "%s %s --host %s --port %s" % (mongodump, auth_string, host, port)
    if directory != ".":
        cmd = "cd %s && %s" % (directory, cmd)
    run_cmd(cmd, abort=True, norun=Norun)
    print "  done."
    
def export_additional_data(mongoexport, auth_string, host, port, dump_dir, caseid):
    '''
    Export additional data.
    Add "text" version of some collections, to make it easier for the
    customer to review the sensitive data.
    Also, add some documents to identify this database once imported.
    :param mongoexport: path to "mongoexport".
    :param host: host where the source MMS instance is. Default to localhost.
    :param port: port to access the database. Default to 27017.
    :param dump_dir: dump directory in which the additional data will be added.
    :param caseid: used to prefix the groups, so we don't have collisions in
                   the receiving database.
    '''
    print "Exporting additional collections"
    for db_coll in COLLECTIONS_TO_EXPORT:
        (db, coll) = db_coll
        json_file = os.path.join(dump_dir, COLLECTIONS_DIR, db, coll)
        cmd = "%s %s --host %s --port %s -d %s -c %s -o %s" % (mongoexport, auth_string, host, port, db, coll, json_file)
        run_cmd(cmd, norun=Norun, abort=True)
        # Modify the customer group names, so they have the case ID as a prefix
        if not Norun:
            if db == COLLECTION_WITH_GROUPS[0] and coll == COLLECTION_WITH_GROUPS[1]:
                replace_string(json_file, '"n" : "', '"n" : "%s-' % (caseid))
    
def get_avail_space(directory):
    '''
    Return the available space on the target directory where we will
    export the data.
    Return the available disk space in MB
    :param directory: for which we want the disk space available.
    '''
    s = os.statvfs(directory)
    df = (s.f_bavail * s.f_frsize) / (1024 * 1024)
    if Verbose:
        print "Space available on disk: %d MB" % (df)
    return df

def get_dbs_space(mongoshell, auth_string, host, port):
    '''
    Get the space used by all DBs we want to export
    :param mongoshell: path to the mongoshell command.
    :param host: host where the DB is located.
    :param port: port to access the DB.
    '''
    dbs_space = 0
    unexpected_dbs = []

    # Get the list of DBs
    cmd = "db.adminCommand('listDatabases').databases"
    (_, out) = run_mongoshell_cmd(mongoshell, auth_string, host, port, "test", cmd)
    # Iterate through the DBs
    # If MMS DB, add it, if not and big, warn that this may not work...
    mms_dbs = 0
    for one_line in out:
        m = re.search(r'"name"\s*:\s*"(.+)"', one_line)
        if m:
            one_db = m.group(1)
            identified_db = False
            for ok_db in ALL_MMS_DBS:
                if re.search(ok_db, one_db):
                    # Add the space
                    cmd = "db.stats().dataSize"
                    (_, out) = run_mongoshell_cmd(mongoshell, auth_string, host, port, one_db, cmd)   
                    one_db_space = int(out[0].rstrip())/(1024*1024)     
                    dbs_space += one_db_space     
                    if Verbose:
                        print "DB: %s, %d MB" % (one_db, one_db_space)
                    identified_db = True
                    mms_dbs += 1
                    break
            for not_db in IGNORE_DBS:
                if re.search(not_db, one_db):
                    # Nothing to do with those
                    identified_db = True
                    break
            if identified_db == False:
                if Verbose:
                    warning("Unexpected DB on the MMS server: %s" % (one_db))
                unexpected_dbs.append(one_db)   
                if len(unexpected_dbs) >= MAX_UNEXPECTED_DBS:
                    fatal("Too many unexpected DBs, will not export unless you run with --nocheck\n  unexpected DBs: %s" % (unexpected_dbs,))         
    if mms_dbs < MIN_EXPECTED_DBS:
        fatal("Did not encountered enough MMS databases. If you are sure it is a good DB, you can re-run with the --nocheck option")
    if Verbose:
        print "Databases space on disk: %d MB" % (dbs_space)
    return dbs_space

def get_mms_version(dump_dir):
    '''
    Identify the MMS version.
    Because we may not be on the MMS server itself, we don't have access to
    the MMS binaries.
    Currently, we are looking at the migrations run by MMS. Each version of
    MMS is running migrations between its versions, at least up to now.
    FIXME - If a new version does not do migrations, this algorithm will be
    broken.
    :param dump_dir: root dir from which we find the collections used to
                     deduce the version number.
    '''
    # Use the migration rules on the DB to figure out the version
    # Every new MMS version changes a little the schema, and we add rules that are
    # cumulative to update the schema, so we use the fact that those rules were run
    # to figure out the version
    version = "1.0"
    migration_file = os.path.join(dump_dir, COLLECTIONS_DIR, "cloudconf", "app.migrations")
    (ret, out) = run_cmd("wc -l %s" % (migration_file), array=False, norun=Norun, abort=True)
    if not ret:
        items = out.split()
        count = items[0]
        if count == '0':
            version = "1.1"
        elif count == '1':
            version = "1.2"
        elif count == '11':
            version = "1.3"
        else:
            # For version we don't know yet
            version = count
    if Verbose:
        print "MMS version is %s" % (version)
    return version
    
def package(directory, zipname):
    '''
    Create a Zip file of the data.
    :param directory: directory to Zip
    :param zipname: CS-xxxxx case the customer has open with us in case
                    the file is shipped, otherwise 'mongo_mms_data'.
    '''
    print "Packaging...",
    target = os.path.join(directory, zipname + ".gzip")
    tar = tarfile.open(target, "w:gz")
    tar.add(os.path.join(directory, DUMPDIR))    
    tar.close()
    print "  done."
    return target
    
def run_mongoshell_cmd(mongoshell, auth_string, host, port, db, cmd, norun=Norun):
    '''
    Run a command in the Mongo shell and return the result as an
    array of lines.
    Unfortunately, this is to avoid using PyMongo, so the result
    is unstructured and the caller must process the lines.
    :param mongoshell: path to 'mongo' shell
    :param host: host to connect to
    :param port: port to connect to
    :param db: db on which the command is ran
    :param cmd: MongoDB shell command to run
    :param norun: Optional parameter to not run the command, but
                  just show what would be ran.
    '''
    mongoshell_cmd = "%s %s --quiet --host %s --port %s --eval \"printjson(%s)\" %s" % (mongoshell, auth_string, host, port, cmd, db)
    status, out = run_cmd(mongoshell_cmd, norun=norun, abort=True)
    # Work around an issue when bad authentication returns 0
    if not auth_string:
        if type(out) is str and out == "undefined":
            status = 1
            raise Exception("ERROR in running - %s\n%s" % (mongoshell_cmd, out))        
        elif len(out) > 0 and out[0] == "undefined":
            status = 1
            raise Exception("ERROR in running - %s\n%s" % (mongoshell_cmd, out[0]))
    return status, out

def safe_rm_tree(directory):
    '''
    Just a wrapper on 'shutil.rmtree', to show that it is safe.
    The script will ensure that we don't remove anything we should not
    :param directory: directory to remove
    '''
    if not re.search(DUMPDIR, directory):
        fatal("Unexpected directory to remove: %s" % (directory))
    shutil.rmtree(directory)

def ship(zipfile, caseid):
    '''
    scp the zip file to the MongoDB DropBox
    :param zipfile: name of the file to ship
    :param caseid: caseid under which it will be copied in DropBox
    '''
    print "Preparing to upload to MongoDB Inc"
    print "  *** You will be prompted to enter a password, just press <enter> ***"
    print ""
    cmd = 'scp -o "StrictHostKeyChecking no" -P 722 %s %s%s@www.mongodb.com:.' % (zipfile, FTP_PREFIX, caseid)
    run_cmd(cmd, abort=True, norun=Norun)
    os.remove(zipfile)
    print "  done."

def write_import_data(dump_dir):
    '''
    Write some additional data regarding this export, so it can be tracked
    and search in the target MMS database.
    :param dump_dir: directory where to create the file with this info
    '''
    db_dir = os.path.join(dump_dir, COLLECTIONS_DIR, IMPORTER_LOGS[0])
    if os.path.exists(db_dir):
        safe_rm_tree(db_dir)
    os.mkdir(db_dir)
    doc = dict()
    now = int(round(time.time() * 1000))
    doc['export_ts'] = '{"$date":%d}' % (now)
    doc['export_host'] = '"%s"' % (socket.gethostname())
    if not Norun:
        data_file = open(os.path.join(db_dir, IMPORTER_LOGS[1]), 'w')
        data_file.write(doc_to_json(doc) + "\n")
        data_file.close()
    
def write_mms_version(dump_dir):
    '''
    Write the MMS version in a file, so the importer knows how to import the data.
    :param dump_dir: dir under which the version file is saved.
    '''
    if not Norun:
        mms_version = get_mms_version(dump_dir)
        ver_file = open(os.path.join(dump_dir, MMS_VERSION_FILE),'w')
        ver_file.write(mms_version)
        ver_file.close()
    
def main():
    '''
    The main module.
    '''
    if os.name == 'nt':
        fatal("This script has not been ported on Windows yet. It runs on Linux and Mac")
    sys.stdout = flushfile(sys.stdout)
    (options, args) = get_opts()
    if args:
        fatal("Found trailing arguments: %s" % (str(args)))
    if options.verbose:
        global Verbose
        Verbose = True
        print "Verbose mode on, will show more info..."
        print "%s version %s" % (TOOL, VERSION)
        print "Running Python version %s" % (sys.version)
    if options.norun:
        global Norun
        Norun = True
    if (options.ship or options.zip) and not options.caseid:
        fatal("You must provide a '-caseid' in order to ship or create a shippable package")
    auth_string = ''
    if options.username or options.password:
        if not options.username or not options.password:
            fatal("You must provide both: --username and --password")
        else:
            auth_string = "--username %s --password %s --authenticationDatabase %s" % (options.username, options.password, AUTH_DB)
    try:
        options.host = get_host(options.host)
        dump_dir = os.path.join(options.directory, DUMPDIR)
        if os.path.exists(dump_dir):
            if options.force:
                safe_rm_tree(dump_dir)
            else:
                fatal("You must use '--force' OR remove manually the directory: %s" % (dump_dir))
        paths = find_paths(DEPS)
        if not options.nocheck:
            space_avail = get_avail_space(options.directory)
            if space_avail < MIN_DISK_SPACE:
                fatal("Disk should have at least ~%d MBytes free, there is only %d MBytes available on disk" % (MIN_DISK_SPACE, space_avail))
            space_dbs = get_dbs_space(paths['mongo'], auth_string, options.host, options.port)
            # We need 1x for the data, 1x or less for the zip, and we give ourselves some margin
            space_needed = space_dbs * 3
            if space_avail < space_needed:
                fatal("Export needs ~%d MBytes free, there is only %d MBytes available on disk" % (space_needed, space_avail))
        dump_database(paths['mongodump'], auth_string, options.host, options.port, options.directory)
        clean_dumped_data(dump_dir)
        export_additional_data(paths['mongoexport'], auth_string, options.host, options.port, dump_dir, options.caseid)
        write_mms_version(dump_dir)
        write_import_data(dump_dir)
        if options.ship:
            zipfile = package(options.directory, options.caseid)
            ship(zipfile, options.caseid)
        elif options.zip:
            zipfile = package(options.directory, options.caseid)
            
    except Exception, e:
        error("caught exception:\n")
        traceback.print_exc()
    if Errors:
        print "The script terminated with errors"
    else:
        print "Done."
        
# Common functions
# Those are shared with 'mongo_mms_import', so changes here should be done
# tested in the other script.
def error(mes):
    '''
    Print an error message, and count the errors
    :param mes: message to print
    '''
    global Errors
    Errors += 1
    print "\nERROR - %s" % (mes)
    return

def fatal(mes):
    '''
    Print a fatal message and exit
    :param mes: message to print
    '''
    global Errors
    Errors += 1
    print "\nFATAL - %s" % (mes)
    os.sys.exit(100)

def warning(mes):
    '''
    Print a warning
    :param mes: message to print
    '''
    print "WARNING - %s" % (mes)
    return

def find_paths(deps):
    '''
    Find the paths of all MongoDB tools we need to export the DB.
    :param deps: list of all the tools we depend on and want to resolve
                 to full paths.
    '''
    paths = dict()
    errors = 0
    MONGO_HOME = 'MONGO_HOME'
    for one_dep in deps:
        if os.environ.get(MONGO_HOME):
            dep = os.path.join(os.environ.get(MONGO_HOME), 'bin', one_dep)
        else:
            dep = one_dep
        cmd = dep + " --version"
        (ret, out) = run_cmd(cmd)
        if ret:
            errors += 1
            error("can't find %s, you can add it to your path or set %s" % (dep, MONGO_HOME))
        else:
            if Verbose:
                print "Found %s" % (out)
            paths[one_dep] = dep
    if errors:
        fatal("aborting...")
    return paths

def get_host(hostname):
    '''
    Utility function to look into your local hosts file to see
    if you want to use an alias for the host.
    :param hostname:
    '''
    if os.path.exists(HOSTS_FILE):
        hosts_file = open(HOSTS_FILE, "r" )
        for line in hosts_file:    
            items = line.split()
            if len(items) >= 2 and items[0] == hostname:
                hostname = items[1]
                break    
    return hostname

def replace_string(filename, search_exp, replace_exp):
    '''
    Utility to replace a string in a file.
    :param filename: file to modify
    :param search_exp: string to be replaced
    :param replace_exp: replacement string
    '''
    for line in fileinput.input(filename, inplace=1):
        if search_exp in line:
            line = line.replace(search_exp, replace_exp)
        sys.stdout.write(line)

def run_cmd(cmd, array=True, abort=False, norun=False):
    '''
    Run a command in the shell and return the result as a string or list
    :param cmd: command to run
    :param array: optional, return result as array. 'True' is the default
    :param abort: if True, abort the command if a failure occur
    :param norun: don't run the command, just show what would be ran.
    '''
    if norun:
        print "Would run CMD: ", cmd
        status = 0
        if array:
            out = []
        else:
            out = ""
    else:
        if Verbose:
            print "Running CMD: %s" % (cmd)
        (status, out) = commands.getstatusoutput(cmd)
        if status != 0:
            if abort:
                raise Exception("ERROR in running - %s\n%s" % (cmd, out))
        if array == True:
            return status, out.split('\n')
    return status, out

# Utility classes
class flushfile(object):
    '''
    Class to flush STDOUT and STDERR
    '''
    def __init__(self, f):
        self.f = f

    def write(self, x):
        self.f.write(x)
        self.f.flush()

if __name__ == '__main__':
    main()



