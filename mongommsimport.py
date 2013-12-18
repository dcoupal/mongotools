#!/usr/bin/env python

'''
Created on Dec 18, 2013

@author: Daniel Coupal

Script to restore an MMS instance.
  - it connects to the MMS host to receive the data
  - explodes the .gzip file
  - clean the data
    - remove the MMS configuration, so we don't overwrite the target
    - create a dummy user named 'ec-XXXXX', so we can connect to the data
  - restore the data with 'mongorestore'
  - creates an entry about that restore, so we get the a trace of the import, the time, ...
  
Pre-requisites:
  - Python < 2.3 and > 3.0
  - user must have 'mongo' and 'mongodump' in path, or set MONGO_HOME

Implementation details:
  - 

 TODOs
  - 
'''

import commands
import optparse
import os
import shutil
import sys
import tarfile

TOOL = "mongommsimport"
VERSION = "0.1.0"

DEPS = [ "mongo", "mongorestore" ]
DUMPDIR = "dump"
PID = os.getpid()

Errors = 0
Verbose = False

def get_opts():
    # TODO - provide a better usage template
    parser = optparse.OptionParser(version="%prog " + VERSION)
    group_general = optparse.OptionGroup(parser, "General options")
    parser.add_option_group(group_general)
    group_general.add_option("-d", "--directory", dest="directory", type="string", default=".", help="temporary dir to use for the restore", metavar="DIR")
    group_general.add_option("-f", "--file", dest="file", type="string", default="", help="name of the .gzip file to import", metavar="FILE")
    group_general.add_option("--host", dest="host", type="string", default='localhost', help="host name of the MMS server", metavar="HOST")
    group_general.add_option("-p", "--port", dest="port", type="string", default='27017', help="port of the MMS server", metavar="PORT")
    group_general.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="show more output")
    group_security = optparse.OptionGroup(parser, "Security options")
    parser.add_option_group(group_security)
    group_security.add_option("-n", "--norun", dest="norun", action="store_true", default=False, help="don't run, just show what would be run")
    (options, args) = parser.parse_args()
    return options, args

def clean_data(directory):
    # TODO
    # Remove the MMS config data
    pass

def explode_gzip(gzipfile, target_dir):
    print "Exploding gzip file...",
    tar = tarfile.open(gzipfile, "r:gz")
    tar.extractall(path=target_dir)    
    tar.close()
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
    
def restore_database(host, port, directory, mongorestore):
    print "Restoring database...",
    cmd = "%s --host %s --port %s" % (mongorestore, host, port)
    cmd = "cd %s & %s" % (directory, cmd)
    run_cmd(cmd, abort=True)
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
        paths = find_paths(DEPS)
        dump_dir = os.path.join(options.directory, PID)
        if os.path.exists(dump_dir):
            warning("Had to remove previously left over temp dir: %s" % (dump_dir))
        if not os.path.exists(options.file):
            fatal("Can't find gzip file to import: %s" % (options.file))
        explode_gzip(options.file, dump_dir)
        if not os.path.exists(dump_dir):
            fatal("Can't find the dump directory to restore from: %s" % (dump_dir))
        clean_data(dump_dir)
        restore_database(options.host, options.port, dump_dir, paths['mongorestore'])
        # Clean the dump tree
        shutil.rmtree(dump_dir)
            
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

