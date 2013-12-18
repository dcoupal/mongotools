#!/usr/bin/env python

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
  - The script should not 'rm' anything, instead it will tell the user to remove things in the way
  
 TODOs
  - calculate DB and disk space
  - add --force to overwrite the 'dump' dir?
  - better check on the disk needs
  - implement '-norun'
'''

import commands
import optparse
import os
import sys
import tarfile

TOOL = "mongommsexport"
VERSION = "0.1.0"

DEPS = [ "mongo", "mongodump" ]
DUMPDIR = "dump"

Errors = 0
Verbose = False

def get_opts():
    # TODO - provide a better usage template
    parser = optparse.OptionParser(version="%prog " + VERSION)
    group_general = optparse.OptionGroup(parser, "General options")
    parser.add_option_group(group_general)
    group_general.add_option("-d", "--directory", dest="directory", type="string", default=".", help="directory where to put the tar file", metavar="DIR")
    group_general.add_option("--host", dest="host", type="string", default='localhost', help="host name of the MMS server", metavar="HOST")
    group_general.add_option("-p", "--port", dest="port", type="string", default='27017', help="port of the MMS server", metavar="PORT")
    group_general.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="show more output")
    group_security = optparse.OptionGroup(parser, "Security options")
    parser.add_option_group(group_security)
    group_security.add_option("-c", "--clean", dest="clean", action="store_true", default=False, help="remove proprietary information")
    group_security.add_option("-n", "--norun", dest="norun", action="store_true", default=False, help="don't run, just show what would be run")
    group_security.add_option("-s", "--ship", dest="ship", type="string", help="ship the data under the given case ID number", metavar="CASEID")
    (options, args) = parser.parse_args()
    return options, args

def clean_data(directory, deep_clean):
    # TODO
    if directory:
        data_dir = os.path.join(directory, DUMPDIR)
    else:
        data_dir = os.path.join('.', DUMPDIR)
    if deep_clean:
        fatal("Cleaning the data is not implemented yet, you need to do it manually.\n" +
              "Files are in %s" % (data_dir))

def dump_database(host, port, directory, mongodump):
    print "Dumping database...",
    cmd = "%s --host %s --port %s" % (mongodump, host, port)
    if directory != ".":
        cmd = "cd %s & %s" % (directory, cmd)
    run_cmd(cmd, abort=True)
    print " done."
    
def find_paths(deps):
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
    # TODO
    space = 0
    return space

def get_dbs_space(host, port):
    # TODO
    space = 0
    return space

def package_and_ship(directory, caseid):
    print "Packaging...",
    target = os.path.join(directory, caseid + ".gzip")
    tar = tarfile.open(target, "w:gz")
    tar.add(os.path.join(directory, DUMPDIR))    
    tar.close()
    print " done."
    print "Uploading to MongoDB Inc...",
    
    print " done."
    
def main():
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
            fatal("You must remove manually the directory: %s" % (dump_dir))
        paths = find_paths(DEPS)
        space_needed = get_dbs_space(options.host, options.port)
        space_avail = get_avail_space(options.directory)
        # TODO - need a better formula, since we have the 'dump' dir and the 'gzip' files to create
        if space_avail < space_needed:
            print "Database is %d MBytes, there is only %s MBytes available on disk" % (space_needed/1000, space_avail/1000)
        dump_database(options.host, options.port, options.directory, paths['mongodump'])
        clean_data(options.directory, options.clean)
        if options.ship:
            package_and_ship(options.directory, options.ship)
            
    except Exception, e:
        error("caught exception:\n  " + e.__str__())
    
# Common functions
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



