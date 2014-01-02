#!/usr/bin/env python2.7

'''
Created on Dec 17, 2013

@author: Daniel Coupal

Script to export an MMS instance.
  - it connects to the MMS host
  - calculates the size of the data to dump and ensure we have enough space
  - dump all data with 'mongodump'
  - parse the data to remove some potential sensitive data
  - tar the resulting file
  - copy the resulting file on our FTP server

Pre-requisites:
  - Python < 2.3 and > 3.0
  - user must have 'mongo' and 'mongodump' in path, or set MONGO_HOME

Implementation details:
  - The --clean option should
    - remove users, or at least remove user names from email addresses
    - remove alerts
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
  - implement '-norun'
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

MMS_3_0_MIG_RULE = "CreateUserRolesPhase1"  # This is one of the 10 rules for the 2.0->3.0 migration
MMS_2_0_MIG_RULE = "SplitRrdMinuteCollections"

COLLECTIONS_DIR = "_collections"
DEPS = ("mongo", "mongodump", "mongoexport")
DUMPDIR = "dump"
FTP_PREFIX = "MMS-"
NUL_DOMAIN = "example.com"

FILES_TO_REMOVE = ["cloudconf/app.migrations.bson",
                   "mmsdb/data.emails.bson",
                   "mmsdbconfig/config.alertSettings.bson",
                   "mmsdbconfig/config.customers.bson",
#                   "mmsdbconfig/config.users.bson"    # FIXME, if we remove users, we have problems giving access to the group, because the main user authorize the access.
                   ]
COLLECTIONS_TO_EXPORT = [ ("mmsdbconfig", "config.customers") ]

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
    group_security.add_option("-c", "--clean", dest="clean", action="store_true", default=False, help="remove proprietary information")
    group_security.add_option("-n", "--norun", dest="norun", action="store_true", default=False, help="don't run, just show what would be run")
    group_security.add_option("-s", "--ship", dest="ship", type="int", default=0, help="ship the data under the given case ID number, for example 12345 for the case ID ec-12345", metavar="nnnnn")
    (options, args) = parser.parse_args()
    return options, args

def clean_dumped_data(directory, deep_clean):
    '''
    Remove some directories and files from the "dump" directory.
    Those paths are mostly info the customer does not want to send, or data we
    don't want to load in the target MMS instance. For example data like
    settings could overwrite the data in the instance. In general any
    data that is shared by many MMS instances should not be loaded.
    :param directory: where the "dump" dir is located
    :param deep_clean: Not Implemented, but would do more obfuscation, ...
    '''
    print "Removing sensitive information like user, emails, ..."
    if directory:
        data_dir = os.path.join(directory, DUMPDIR)
    else:
        data_dir = os.path.join('.', DUMPDIR)
    for one_file in FILES_TO_REMOVE:
        file_to_rm = os.path.join(directory, one_file)
        os.remove(file_to_rm)
    if deep_clean:
        fatal("Cleaning the data is not implemented yet, you need to do it manually.\n" +
              "Files are in %s" % (data_dir))

def dump_database(host, port, directory, mongodump):
    '''
    Dump the database with "mongodump".
    :param host: host where the source MMS instance is. Default to localhost.
    :param port: port to access the database. Default to 27017.
    :param directory: directory where to dump to database.
    :param mongodump: path to the executable mongodump.
    '''
    print "Dumping database...",
    cmd = "%s --host %s --port %s" % (mongodump, host, port)
    if directory != ".":
        cmd = "cd %s && %s" % (directory, cmd)
    run_cmd(cmd, abort=True)
    print " done."
    
def export_additional_data(mongoexport, dump_dir, caseid):
    '''
    Export additional data.
    Add "text" version of some collections, to make it easier for the
    customer to review the sensitive data.
    Also, add some documents to identify this database once imported.
    :param mongoexport: path to "mongoexport".
    :param dump_dir: dump directory in which the additional data will be added.
    '''
    print "Exporting additional collections"
    for db_coll in COLLECTIONS_TO_EXPORT:
        (db, coll) = db_coll
        json_file = os.path.join(dump_dir, COLLECTIONS_DIR, db, coll)
        cmd = "%s -d %s -c %s -o %s" % (mongoexport, db, coll, json_file)
        run_cmd(cmd)
        if db == "mmsdbconfig" and coll == "config.customers":
            replace_string(json_file, '"n" : "', '"n" : "%i-' % (caseid))
    # Modify the customer group names, so they have the case ID as a prefix
    
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
    So, the way to identify the version is to look at the data format in the
    database.
    Currently, we are looking at the migrations run by MMS. Each version of
    MMS is running migrations between its versions, at least up to now.
    TOFIX - If a new version does not do migrations, this algorithm will be
    broken.
    :param dump_dir:
    '''
    # Use the migration rules on the DB to figure out the version
    # Every new MMS version changes a little the schema, and we add rules that are
    # cumulative to update the schema, so we use the fact that those rules were run
    # to figure out the version
    # FIXME, just don't look for a specific migration, count them, so it is upward
    # compatible.
    version = "1.0"
    migration_file = "cloudconf/app.migrations.bson"
    (ret, _) = run_cmd("grep '%s' %s/%s" % (MMS_3_0_MIG_RULE, dump_dir, migration_file))
    if not ret:
        version = "1.3"
    (ret, _) = run_cmd("grep '%s' %s/%s" % (MMS_2_0_MIG_RULE, dump_dir, migration_file))
    if not ret:
        version = "1.2"
    if Verbose:
        print "MMS version is %s" % (version)
    return version
    

def package_and_ship(directory, caseid):
    '''
    Create a Zip file of the data and send it to the FTP site.
    FIXME: still have issue in doing the 'scp' because the remote site is
           asking for a password, even if it is blank.
    :param directory: directory to Zip
    :param caseid: CS-xxxxx case the customer has open with us.
                   We use that case number as the user for the scp.
    '''
    print "Packaging...",
    target = os.path.join(directory, str(caseid) + ".gzip")
    tar = tarfile.open(target, "w:gz")
    tar.add(os.path.join(directory, DUMPDIR))    
    tar.close()
    print " done."
    print "Preparing to upload to MongoDB Inc"
    print "  *** You will be prompted to enter a password, just press <enter> ***"
    print ""
    cmd = 'scp -o "StrictHostKeyChecking no" -P 722 %s %s%d@www.mongodb.com:.' % (target, FTP_PREFIX, caseid)
    run_cmd(cmd, abort=True)
    os.remove(target)
    print " done."
    
def replace_string(filename, search_exp, replace_exp):
    for line in fileinput.input(filename, inplace=1):
        if search_exp in line:
            line = line.replace(search_exp, replace_exp)
        sys.stdout.write(line)

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
        dump_database(options.host, options.port, options.directory, paths['mongodump'])
        mms_version = get_mms_version(dump_dir)
        clean_dumped_data(dump_dir, options.clean)
        export_additional_data(paths['mongoexport'], dump_dir, options.ship)

        if options.ship:
            package_and_ship(options.directory, options.ship)
            
    except Exception, e:
        error("caught exception:\n  " + e.__str__())
    
# Common functions
# Those are shared with 'mongommsimport', so changes here should be done
# in the other script.
def error(mes):
    global Errors
    Errors += 1
    print "ERROR - %s" % (mes)
    return

def fatal(mes):
    global Errors
    Errors += 1
    print "FATAL - %s" % (mes)
    os.sys.exit(100)

def warning(mes):
    print "WARNING - %s" % (mes)
    return

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
    def __init__(self, f):
        self.f = f

    def write(self, x):
        self.f.write(x)
        self.f.flush()

if __name__ == '__main__':
    main()
    pass



