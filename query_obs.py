#!/usr/bin/env python

import os,sys
import glob
import argparse
import datetime

parser = argparse.ArgumentParser(description="Query observations")
parser.add_argument("-utc1",dest="utc1", help="Start UTC",required=True)
parser.add_argument("-utc2",dest="utc2", help="End UTC",required=True)
parser.add_argument("-pid", dest="pid",help="PID")
parser.add_argument("-psr",dest="pulsar",help="Pulsar name")
parser.add_argument("-outfile", dest="outfile", help="Output file")
args = parser.parse_args()


path = "/fred/oz005/timing"
utc1 = datetime.datetime.strptime(args.utc1, '%Y-%m-%d-%H:%M:%S')
utc2 = datetime.datetime.strptime(args.utc2, '%Y-%m-%d-%H:%M:%S')

def get_obsheadinfo(obsheader_path):
    """
    Parse the obs.header file and return important parameters
    """
    params={}
    with open(obsheader_path) as file:
        lines=file.readlines()
        for line in lines:
            (key, val) = line.split()
            params[str(key)] = str(val).rstrip()

    file.close()
    return params


pulsar_list = sorted(glob.glob(os.path.join(path,"J*")))

def get_pid_dir(pid):
    #Routine to return a readable PID directory for the pipeline

    if pid == "SCI-20180516-MB-01":
        pid_dir = "MB01"
    elif pid == "SCI-20180516-MB-02":
        pid_dir = "TPA"
    elif pid == "SCI-20180516-MB-03":
        pid_dir = "RelBin"
    elif pid == "SCI-20180516-MB-04":
        pid_dir = "GC"
    elif pid == "SCI-20180516-MB-05":
        pid_dir = "PTA"
    elif pid == "SCI-20180516-MB-06":
        pid_dir = "NGC6440"
    elif pid == "SCI-20180516-MB-99":
        pid_dir = "fluxcal"
    elif pid == "None":
        pid_dir = "None"
    else:
        pid_dir = "Rogue"

    return pid_dir

for pulsar in pulsar_list:
    psrpath,psrname = os.path.split(pulsar)
    if args.pulsar:
        if psrname == str(args.pulsar):
            obs_list = sorted(glob.glob(os.path.join(pulsar,"2*")))
            for obs in obs_list:
                obspath,obsname = os.path.split(obs)
                obname = obsname
                obsname = datetime.datetime.strptime(obsname, '%Y-%m-%d-%H:%M:%S')
                if (obsname-utc1).total_seconds() >= 0 and (obsname-utc2).total_seconds() <= 0:
                    freqs = sorted(glob.glob(os.path.join(obs,"*")))
                    for freq in freqs:
                        beams = sorted(glob.glob(os.path.join(freq,"*")))
                        for beam in beams:
                            header_params = get_obsheadinfo(os.path.join(beam,"obs.header"))
                            pid = get_pid_dir(header_params["PID"])
                            if args.pid:
                                if args.pid == pid:
                                    print "{0} {1} {2}".format(psrname,obname,pid)
                                    if args.outfile:
                                        fname = os.path.join(args.outfile,"{0}_{1}_{2}.list".format(psrname,args.utc1,args.utc2))
                                        f= open(fname,"a")
                                        f.write("{0} {1} {2} \n".format(psrname,obname,pid))
                            
                            else:
                                #print "{0} {1}".format(psrname,obname)
                                if args.outfile:
                                    fname = os.path.join(args.outfile,"{0}_{1}_{2}.list".format(psrname,args.utc1,args.utc2))
                                    f= open(fname,"a")
                                    f.write("{0} {1} \n".format(psrname,obname))

    else:
        obs_list = sorted(glob.glob(os.path.join(pulsar,"2*")))
        for obs in obs_list:
            obspath,obsname = os.path.split(obs)
            obname = obsname
            obsname = datetime.datetime.strptime(obsname, '%Y-%m-%d-%H:%M:%S')
            if (obsname-utc1).total_seconds() >= 0 and (obsname-utc2).total_seconds() <= 0:
                freqs = sorted(glob.glob(os.path.join(obs,"*")))
                for freq in freqs:
                    beams = sorted(glob.glob(os.path.join(freq,"*")))
                    for beam in beams:
                        header_params = get_obsheadinfo(os.path.join(beam,"obs.header"))
                        pid = get_pid_dir(header_params["PID"])
                        if args.pid:
                            if args.pid == pid:
                                print "{0} {1} {2}".format(psrname,obname,pid)
                        else:
                            print "{0} {1}".format(psrname,obname)

