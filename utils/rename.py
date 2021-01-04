#!/usr/bin/env python

import numpy
import os
import sys
import glob


path = "/fred/oz002/meertime/meerpipe/ppta_zap_results/ppta_zap_template/manual_zapped"

files = sorted(glob.glob(os.path.join(path,"J*")))

for ar in files:
    fpath,fname = os.path.split(ar)
    sname = fname.split('_')[1]+"_"+fname.split('_')[2]
    rename = sname+"_zap.pazi"
    os.rename(ar, os.path.join(fpath,rename))
