#!/usr/bin/env python

import argparse
import sys
import os


parser = argparse.ArgumentParser(description="convert to a simpler version for MK observing")
parser.add_argument("-i", dest="inputfile", help="par file")
args=parser.parse_args()

if os.path.exists(str(args.inputfile)):
    path,name = os.path.split(str(args.inputfile))
    with open(os.path.join(path,"{0}_mk.par".format(name)),'w') as par:
        with open(str(args.inputfile)) as old_par:
            for line in old_par.readlines():
                sline=line.split()
                if sline[0] == "MODE":
                    par.write("MODE 1 \n")
                elif sline[0] == "START":
                    par.write("START {0} \n".format(sline[1]))
                elif sline[0] == "FINISH":
                    par.write("FINISH {0} \n".format(sline[1]))
                elif sline[0] == "EPHVER":
                    par.write("EPHVER 5 \n")
                elif sline[0] == "UNITS":
                    par.write("UNITS TCB \n")
                elif sline[0] == "TIMEEPH":
                    par.write("TIMEEPH IF99 \n")
                elif sline[0] == "T2CMETHOD":
                    par.write("T2CMETHOD IAU2000B \n")
                elif sline[0] == "EPHEM":
                    par.write("EPHEM DE436 \n")
                elif sline[0] == "JUMP":
                    pass
                elif sline[0] == "NTOA":
                    pass
                elif sline[0] == "CHI2R":
                    pass
                else:
                    par.write(' '.join(sline))
                    par.write("\n")
else:
    print ("File does not exist")
