#!/usr/bin/python

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
  - calculate DB and disk space
  - better check on the disk needs
  - because we are running 'mongodump', we don't have control on the DBs we
    are exporting. If users have more than MMS in the DB, we may export way 
    too much. Then we would need to export with 'mongoexport'
'''

import commands
import fileinput
import optparse
import os
import shutil
import sys
import tarfile

TOOL = "mongommsexport"
VERSION = "0.1.0"

COLLECTIONS_DIR = "_collections"
DB_CLOUDCONF = "cloudconf"
DB_MMSCONF = "mmsdbconfig"
DEPS = ("mongo", "mongodump", "mongoexport")
DUMPDIR = "dump"
FTP_PREFIX = "MMS-"
MMS_VERSION_FILE = "mms_version"
NUL_DOMAIN = "example.com"

FILES_TO_REMOVE = [
                   "cloudconf/app.migrations.bson",
                   "mmsdb/data.emails.bson",
                   "mmsdbconfig/config.alertSettings.bson",
                   "mmsdbconfig/config.customers.bson",
                   "mmsdbconfig/config.users.bson"
                   ]
COLLECTIONS_TO_EXPORT = [ ("cloudconf", "app.migrations"), ("mmsdbconfig", "config.customers")  ]

ALL_MMS_DBS = [ "cloudconf", "mmsdbconfig" ]

# OS - specific?
HOSTS_FILE = "/etc/hosts"

Errors = 0
Verbose = False

def get_opts():
    '''
    Read the options and arguments provided on the command line.
    '''
    parser = optparse.OptionParser(version="%prog " + VERSION)
    group_general = optparse.OptionGroup(parser, "General options")
    parser.add_option_group(group_general)
    group_general.add_option("-d", "--directory", dest="directory", type="string", default=".", help="directory where to put the tar file", metavar="DIR")
    group_general.add_option("-f", "--force", dest="force", action="store_true", default=False, help="force removal of a previous 'dump' directory")
    group_general.add_option("--host", dest="host", type="string", default='localhost', help="host name of the MMS server", metavar="HOST")
    group_general.add_option("-p", "--port", dest="port", type="string", default='27017', help="port of the MMS server", metavar="PORT")
    group_general.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="show more output")
    group_security = optparse.OptionGroup(parser, "Security options")
    parser.add_option_group(group_security)
    group_security.add_option("-s", "--ship", dest="ship", type="int", default=0, help="ship the data under the given case ID number, for example 12345 for the case ID ec-12345", metavar="nnnnn")
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
    for one_file in FILES_TO_REMOVE:
        file_to_rm = os.path.join(directory, one_file)
        os.remove(file_to_rm)

def dump_database(mongodump, host, port, directory, ):
    '''
    Dump the database with "mongodump".
    :param mongodump: path to the executable mongodump.
    :param host: host where the source MMS instance is. Default to localhost.
    :param port: port to access the database. Default to 27017.
    :param directory: directory where to dump to database.
    '''
    print "Dumping database...",
    cmd = "%s --host %s --port %s" % (mongodump, host, port)
    if directory != ".":
        cmd = "cd %s && %s" % (directory, cmd)
    run_cmd(cmd, abort=True)
    print " done."
    
def export_additional_data(mongoexport, host, port, dump_dir, caseid):
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
        cmd = "%s --host %s --port %s -d %s -c %s -o %s" % (mongoexport, host, port, db, coll, json_file)
        run_cmd(cmd)
        # Modify the customer group names, so they have the case ID as a prefix
        if db == "mmsdbconfig" and coll == "config.customers":
            replace_string(json_file, '"n" : "', '"n" : "%i-' % (caseid))
    
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

def get_avail_space(directory):
    '''
    Return the available space on the target directory where we will
    export the data.
    :param directory: for which we want the disk space available.
    '''
    # TODO
    space = 0
    return space

def get_dbs_space(host, port):
    '''
    Calculate the space used by all DBs we want to export
    :param host: host where the DB is located.
    :param port: port to access the DB.
    '''
    # TODO
    space = 0
    return space

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
    (ret, out) = run_cmd("wc -l %s" % (migration_file), array=False)
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
    print " done."
    return target
    
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

def ship(zipfile, caseid):
    '''
    scp the zip file to the MongoDB DropBox
    :param zipfile: name of the file to ship
    :param caseid: caseid under which it will be copied in DropBox
    '''
    print "Preparing to upload to MongoDB Inc"
    print "  *** You will be prompted to enter a password, just press <enter> ***"
    print ""
    cmd = 'scp -o "StrictHostKeyChecking no" -P 722 %s %s%d@www.mongodb.com:.' % (zipfile, FTP_PREFIX, caseid)
    run_cmd(cmd, abort=True)
    os.remove(zipfile)
    print " done."
    
def write_mms_version(dump_dir):
    '''
    Write the MMS version in a file, so the importer knows how to import the data.
    :param dump_dir: dir under which the version file is saved.
    '''
    mms_version = get_mms_version(dump_dir)
    ver_file = open(os.path.join(dump_dir, MMS_VERSION_FILE),'w')
    ver_file.write(mms_version)
    ver_file.close()
    
def main():
    '''
    The main module.
    '''
    global Verbose
    sys.stdout = flushfile(sys.stdout)
    (options, args) = get_opts()
    if args:
        fatal("Found trailing arguments: %s" % (str(args)))
    if options.verbose:
        Verbose = True
        print "Verbose mode on, will show more info..."
        print "%s version %s" % (TOOL, VERSION)
        print "Running Python version %s" % (sys.version)
    try:
        options.host = get_host(options.host)
        dump_dir = os.path.join(options.directory, DUMPDIR)
        if os.path.exists(dump_dir):
            if options.force:
                shutil.rmtree(dump_dir)
            else:
                fatal("You must remove manually the directory: %s" % (dump_dir))
        paths = find_paths(DEPS)
        space_needed = get_dbs_space(options.host, options.port)
        space_avail = get_avail_space(options.directory)
        # TODO - need a better formula, since we have the 'dump' dir and the 'gzip' files to create
        if space_avail < space_needed:
            print "Database is %d MBytes, there is only %s MBytes available on disk" % (space_needed/1000, space_avail/1000)
        dump_database(paths['mongodump'], options.host, options.port, options.directory)
        clean_dumped_data(dump_dir)
        export_additional_data(paths['mongoexport'], options.host, options.port, dump_dir, options.ship)
        write_mms_version(dump_dir)

        if options.ship:
            zipfile = package(options.directory, str(options.ship))
            ship(zipfile, options.ship)
        elif options.zip:
            zipfile = package(options.directory, 'mongodb_mms_data')
            
    except Exception, e:
        error("caught exception:\n  " + e.__str__())
    
# Common functions
# Those are shared with 'mongommsimport', so changes here should be done
# tested in the other script.
def error(mes):
    '''
    Print an error message, and count the errors
    :param mes: message to print
    '''
    global Errors
    Errors += 1
    print "ERROR - %s" % (mes)
    return

def fatal(mes):
    '''
    Print a fatal message and exit
    :param mes: message to print
    '''
    global Errors
    Errors += 1
    print "FATAL - %s" % (mes)
    os.sys.exit(100)

def warning(mes):
    '''
    Print a warning
    :param mes: message to print
    '''
    print "WARNING - %s" % (mes)
    return

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

def run_cmd(cmd, array=True, abort=False):
    '''
    Run a command in the shell and return the result as a string or list
    :param cmd: command to run
    :param array: return result as array. 'True' is the default
    '''
    if Verbose:
        print "Running CMD: ", cmd
    (status, out) = commands.getstatusoutput(cmd)
    if status:
        if abort:
            raise Exception("ERROR in running - " + cmd)
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




