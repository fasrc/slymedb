#!/usr/bin/env python
# encoding: utf-8

"""
Copyright (c) 2014
Harvard FAS Research Computing
All rights reserved.


Created on May 6, 2014

@author: Aaron Kitzmiller
"""

import sys
import os
import getpass
from slyme import JobReport
from slymedb import Store

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

def main(): # IGNORE:C0111
    '''Command line options.'''
#     try:
    # Setup argument parser
    parser = ArgumentParser(description="Load job reports from sacct into db tables", \
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-v","--verbose",action="store_true",help="Increase output")
    parser.add_argument("-u","--user",help="Database username",required=True)
    parser.add_argument("-p","--password",help="Database password")
    parser.add_argument("--host",help="Database hostname",default='localhost')
    parser.add_argument("-d","--database",help="Database",required=True)
    parser.add_argument("--drop-tables",action="store_true",help="If tables exist, drop them first")
    parser.add_argument("--sacct-parameters",
        help="Comma-separated list of sacct parameters, e.g. \
              --sacct-parameters=\"user=akitzmiller,starttime=2014-05-01\"")
    
    # Process arguments
    args = parser.parse_args()
    
    # If password not supplied, prompt for it
    password = args.password
    
    if not password:
        password = getpass.getpass('Database password: ')
    
    # Check the connection to the database
    connectstring = "mysql://%s:%s@%s/%s" % (args.user,password,args.host,args.database)
    
    store = Store(connectstring)
    sys.stderr.write("Connected to database successfully\n")
    
    # Drop and recreate tables if requested
    if args.drop_tables:
        store.drop()
        store.create()
    
    # Parse the sacct parameters into a dict
    sacctparams = {}
    if args.sacct_parameters is not None:
        paramlist = args.sacct_parameters.split(',')
        for param in paramlist:
            kv = param.split('=')
            if len(kv) == 2:
                sacctparams[kv[0]] = kv[1]
            elif len(kv) == 1:
                sacctparams[kv[0]] = True
            else:
                raise Exception("Can't parse sacct parameter %s" % param)
                            
    jrs = JobReport.fetch(**sacctparams)
    count = 0
    for jr in jrs:
        if jr is None:
	    raise Exception("Job report is null")
        store.save([jr])
        count += 1
        if count % 100 == 0:
            sys.stderr.write("Loaded %d job reports\n" % count)
    
    sys.stderr.write("Finished loading %d job reports\n" % count)
    return 0
#     except KeyboardInterrupt:
#         ### handle keyboard interrupt ###
#         return 0
#     except Exception, e:
#         sys.stderr.write(e.message + "\n")
#         return 2

if __name__ == "__main__":
    sys.exit(main())
