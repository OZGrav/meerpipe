#!/usr/bin/env python

"""
Code containing utilities adapted from MeerWatch codebase - /fred/oz005/users/meerpipe_dev/MeerWatch

__author__ = ["Renee Spiewak", "Andrew Cameron"]
__credits__ = ["Aditya Parthasarathy"]
__maintainer__ = "Andrew Cameron"
__email__ = "andrewcameron@swin.edu.au"
__status__ = "Development"
"""


# Imports
import re, os, sys
import numpy as np
from shlex import split as shplit
import subprocess as sproc
import matplotlib.pyplot as plt
from matplotlib import colors, cm


def get_res_fromtim(tim_file, par_file, sel_file=None, out_dir="./", verb=False):

    tempo2_call = "tempo2 -nofit -set START 40000 -set FINISH 99999 "\
                  "-output general2 -s \"{{bat}} {{post}} {{err}} "\
                  "{{freq}} BLAH\n\" -nobs 10000000 -npsr 1 -f {} {}"
    awk_cmd = "awk '{print $1,$2,$3*1e-6,$4}'"
    temp_file = os.path.basename(tim_file).replace('.tim', '_res.txt')
    temp_file = os.path.join(out_dir, temp_file)

    # if a select file is given, include it                                                                                                                                                                                                  
    if sel_file is not None:
        tempo2_call += " -select {}".format(sel_file)

    # call tempo2 to produce residuals that can be read in                                                                                                                                                                               
    with open(temp_file, 'w') as f:
        if verb:
            print("Running tempo2 command: {}".replace("\n", "\\n").format(tempo2_call.format(par_file, tim_file)))

        # re-writing to accommodate large PIPE sizes
        p1 = sproc.Popen(shplit(tempo2_call.format(par_file, tim_file)), stdout=sproc.PIPE)
        p1_data = p1.communicate()[0]
        p2 = sproc.Popen(shplit("grep BLAH"), stdin=sproc.PIPE,stdout=sproc.PIPE)
        p2_data = p2.communicate(input=p1_data)[0]
        p3 = sproc.Popen(shplit(awk_cmd), stdin=sproc.PIPE, stdout=f)
        p3.communicate(input=p2_data)

    if verb:
        print("Finished running tempo2")

    toas = np.loadtxt(temp_file, usecols=(0, 1, 2, 3), dtype=[('mjd', 'f8'), ('res', 'f4'), ('err', 'f4'), ('freq', 'f4')])
    if toas.size == 1:
        if verb:
            print("Only one ToA from {}; skipping".format(tim_file))
        toas = np.array([])

    if len(toas) == 0:
        print("No ToAs from tempo2 for {}".format(tim_file))

    return(toas)


def plot_toas_fromarr(toas, fs=14, out_file="toas.png", out_dir=None, sequential=True, title=None, verb=False, bw=856, cfrq=1284, nchn=None):

    if out_dir:
        out_file = os.path.join(out_dir, out_file)

    # use semi-fixed color normalisation                                                                                                                                                                                                     
    f_min = cfrq-bw/2.0
    f_max = cfrq+bw/2.0

    norm = colors.Normalize(vmin=f_min, vmax=f_max)

    fig = plt.figure(num=1)
    fig.set_size_inches(6, 4.5)
    ax = fig.gca()

    if sequential:
        if verb:
            print("Plotting serial ToAs")

        if nchn is None:
            raise(RuntimeError("Cannot get number of channels"))

        chan = range(nchn)
        freq_mins = [f_min+(i*bw/nchn) for i in chan]
        freq_maxs = [f_min+((i+1)*bw/nchn) for i in chan]
        pulse = 0
        last_chan = -1
        num = []

        for f in toas['freq']:
            for i, mi, ma in zip(chan, freq_mins, freq_maxs):
                if mi < f < ma:
                    if i <= last_chan:
                        pulse += nchn

                    num.append(pulse+i)

                    last_chan = i
                    break

        if len(num) != len(toas['freq']):
            print(num, toas['freq'], freq_mins, freq_maxs)
            raise(RuntimeError("Error determining ToA Number for {}".format(out_file)))

        xdata = np.array(num)
    else:
        if verb:
            print("Plotting against MJD")

        xdata = toas['mjd']

    p2 = ax.scatter(xdata, toas['res']*1e6, s=8, c=toas['freq'], marker='s', norm=norm, cmap='viridis')
    cb = fig.colorbar(p2, ax=ax, fraction=0.1)

    lines = ax.errorbar(xdata, toas['res']*1e6, yerr=1e6*toas['err'], ls='', marker='', ms=1, zorder=0)[2]
    lines[0].set_color(cb.to_rgba(toas['freq']))

    print (xdata)
    spread = xdata.max()-xdata.min()
    xmin = xdata.min()-0.05*spread
    xmax = xdata.max()+0.05*spread
    ax.plot([xmin-0.1*spread, xmax+0.1*spread], [0, 0], ls='--', color='0.5')
    ax.set_xlim(xmin, xmax)

    cb.set_label("Observing frequency (MHz)", rotation=270, size=fs, labelpad=16)
    if sequential:
        ax.set_xlabel("ToA Number", fontsize=fs)
    else:
        ax.set_xlabel("MJD", fontsize=fs)

    ax.set_ylabel("residuals ($\mu$s)", fontsize=fs)
    if title is not None and type(title) is str:
        ax.set_title(title, fontsize=fs+2)

    plt.savefig(out_file, bbox_inches='tight')

    plt.clf()
