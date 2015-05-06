#!/usr/bin/env python
# encoding: utf-8

"""
Copyright (c) 2014
Harvard FAS Research Computing
All rights reserved.

BOOT_FAIL,CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT
Created on May 6, 2014

@author: Aaron Kitzmiller
"""

import sys, os
import datetime
from time import sleep
import getpass
from slyme import JobReport, Slurm
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
    parser.add_argument("--since-last-entry",action="store_true",\
        help="Set starttime parameter to just above the latest Start value")
    parser.add_argument("--sacct-parameters",
        help="Pipe-separated list of sacct parameters, e.g. \
              --sacct-parameters=\"user=akitzmiller|starttime=2014-05-01\"")
    
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
        paramlist = args.sacct_parameters.split('|')
        for param in paramlist:
            kv = param.split('=')
            if len(kv) == 2:
                sacctparams[kv[0]] = kv[1]
            elif len(kv) == 1:
                sacctparams[kv[0]] = True
            else:
                raise Exception("Can't parse sacct parameter %s" % param)
            
    # Find max(Start) in the database and set starttime to Start + 1
    # If there is no table or value do nothing
    count = 0
    if args.since_last_entry:
        try:
            result = store.connection.execute("select max(Start) as maxstart from jobreport")
            row = result.first()
            maxstart = row["maxstart"]
            
            startdate = maxstart + datetime.timedelta(days=-1)
            enddate = startdate + datetime.timedelta(days=1)
        except Exception, e:
            sys.stderr.write("Error getting max Start.  No starttime will be set \
               %s\n" % e.message)
            exit(1)
            
        # One day at a time, Sweet Jesus
        while enddate < datetime.datetime.today():
            sacctparams["starttime"] = str(startdate)
            sacctparams["endtime"] = str(enddate)
            sys.stderr.write("--starttime %s, --endtime %s\n" % (sacctparams['starttime'],sacctparams['endtime']))
            jrs = Slurm.getJobReports(**sacctparams)
            for jr in jrs:
                try:
                    store.save([jr])
                    count += 1
                    if count % 100 == 0:
                        sys.stderr.write("Loaded %d job reports\n" % count)
                except Exception as e:
                    sys.stderr.write("Error saving jobreport %s\n" % e)
            sys.stderr.write("Loaded %d\n" % count)
                    
            startdate = startdate +  datetime.timedelta(days=1)
            enddate = enddate +  datetime.timedelta(days=1)
            sleep(30)
        
    else:                      
        jrs = Slurm.getJobReports(**sacctparams)
        for jr in jrs:
            try:
                store.save([jr])
                sys.stderr.write("Loaded %s\n" % jr.JobID)
                count += 1
                if count % 100 == 0:
                    sys.stderr.write("Loaded %d job reports\n" % count)
            except Exception as e:
                sys.stderr.write("Error saving jobreport %s\n" % e)
        
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
