#!/usr/bin/env python

#Code to split frequency channels from archives - by Mike Keith

import sys
import psrchive
import numpy as np

tgtfreqs=None
for infile in sys.argv[1:]:
    ar = psrchive.Archive_load(infile)
    freqs = ar.get_frequencies()
    if tgtfreqs==None or len(freqs) < len(tgtfreqs):
        tgtfreqs=list(freqs)

print("Chop to {} channels".format(len(tgtfreqs)))
for infile in sys.argv[1:]:
    ar = psrchive.Archive_load(infile)
    oar=ar.clone()

    dd=oar.get_dedispersed()
    if dd:
        oar.dededisperse()
    recheck=True
    while recheck:
        recheck=False
        freqs = oar.get_frequencies()
        for i,f in enumerate(freqs):
            if f in tgtfreqs:
                pass
            else:
                oar.remove_chan(i,i)
                recheck=True
                break
    if dd:
        oar.dedisperse()
    print("unload",infile+".ch")
    oar.unload(infile+".ch")
