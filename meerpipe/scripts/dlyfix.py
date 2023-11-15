import os
import re
import sys
import copy
import datetime
import argparse

from meerpipe.dlyfix_fits import readfitsheader, binarytable, history_class
from meerpipe.data_load import DELAY_CONFIG


class correction:
    def __init__(self,firmware,beconfig,mjd,tbin,freq,bename):

        self.firmware=firmware
        self.mjd=mjd
        self.beconfig=beconfig
        self.tbin=tbin
        self.freq=freq
        self.bename=bename
        self.verbose=0
        self.prop_printed=0
        self.corrections=[]



    def parse(self, lines):
        if self.verbose and self.prop_printed==0:
            print("Properties read from file are:")
            print("MJD: '%s'\nBECONFIG: '%s'\nBENAME: '%s'\nTBIN: '%s'\nFIRMWARE: '%s'\nFREQ: '%s'"%(self.mjd,self.beconfig,self.bename,self.tbin,self.firmware,self.freq))
            self.prop_printed=1
        name="VOID"
        skip=1
        delay=0
        for line in lines:
            line = line.split("#")[0]
            elems=line.split()
            if len(elems) > 0 and elems[0]=="*":
                name=line.strip("* \t\n")
                delay=0
                skip=0
                continue
            if skip==0:
                if len(elems) > 2  and elems[1]=="~=":
                    val=str(getattr(self, elems[0]))
                    if re.match(elems[2], val) is None:
                        # match failed
                        skip=1
                    continue
                if len(elems) > 2  and elems[1]=="!~=":
                    val=str(getattr(self, elems[0]))
                    if re.match(elems[2],val)!=None:
                        # match failed
                        skip=1
                    continue
                if len(elems) > 2  and elems[1]=="<":
                    val=float(getattr(self, elems[0]))
                    if val >= float(elems[2]):
                        # match failed
                        skip=1
                    continue
                if len(elems) > 2  and elems[1]==">":
                    val=float(getattr(self, elems[0]))
                    if not val > float(elems[2]):
                        # match failed
                        skip=1
                    continue
                if len(elems) > 2  and elems[1]=="<=":
                    val=float(getattr(self, elems[0]))
                    if not val <= float(elems[2]):
                        # match failed
                        skip=1
                    continue
                if len(elems) > 2  and elems[1]==">=":
                    val=float(getattr(self, elems[0]))
                    if not val >= float(elems[2]):
                        # match failed
                        skip=1
                    continue
                if (
                    len(elems) > 2
                    and elems[0] == "delay"
                    and elems[1] in ["=", "+=", "-="]
                ):
                    conv=1
                    val=float(elems[2])
                    if len(elems) > 3:
                        if elems[3]=="ms":
                            conv=1e-3
                        elif elems[3]=="us":
                            conv=1e-6
                        elif elems[3]=="ns":
                            conv=1e-9
                        else:
                            conv=float(getattr(self, elems[3]))
                    if self.verbose:
                        print(f"*** Rule {name} matches")

                    if elems[1]=="=":
                        self.corrections = []
                        delay=val*conv
                        if self.verbose:
                            print("*** NOTE: This rule replaces previous rules")
                            print("*** delay set to %g s"%delay)
                    elif elems[1]=="+=":
                        delay+=val*conv
                        if self.verbose:
                            print("*** delay incremented by %g s"%delay)
                    elif elems[1]=="-=":
                        delay-=val*conv
                        if self.verbose:
                            print("*** delay decremented by %g s"%delay)
                        print("")
                    c = {
                        'corr': delay,
                        'msg': name,
                        'val': val,
                        'conv': conv,
                    }
                    self.corrections.append(c)
                    delay=0
                    continue
                if len(line.strip()) > 0:
                    print("Warning: Cannot understand line in delay file:")
                    print(f"'{line.strip()}'")

# corrections
def getcorrection(mainhdr,freq,history,correctionfiles,verbose=0):
    # Parse some useful values...
    mjdobs=float(mainhdr.get("STT_IMJD").val)
    mjdobs+=float(mainhdr.get("STT_SMJD").val)/86400.0
    beconfig=mainhdr.get("BECONFIG").val.strip(" '")
    tbin = history.entries[0]['TBIN']
    bename = mainhdr.get('BACKEND').val.strip(" '")
    firmware = history.entries[0]['PROC_CMD'].strip()

    # Now see if we match any known delays

    corr = correction(firmware,beconfig,mjdobs,tbin,freq,bename)
    corr.verbose=verbose
    for file in correctionfiles:
        f = open(file)
        corr.parse(f.readlines())


    if verbose:
        for c in corr.corrections:
            name=c['msg']
            val=c['val']
            conv=c['conv']
            print(f"*** Applying correction '{name}'")
            if conv != 1:
                print("*** Value = %g * %g s"%(val,conv))
            else:
                print("*** Value = %g s"%(val))

    return corr.corrections




def main():
    parser = argparse.ArgumentParser(description="Corrects the psrfits header start time using the latest correction files.")
    parser.add_argument("-e", "--extension", type=str, help="Output with this extention")
    parser.add_argument("-o", "--output_name", type=str, help="Output to the file to this directory")
    parser.add_argument("-d", "--output_dir", type=str, help="Output to the file with this name")
    parser.add_argument("-m", "--modify", action="store_true", help="Modify file in place. Note: -m option will only work if there is room in the history table to add to it without resizing")
    parser.add_argument("-u", type=str, help="Write to new directory")
    parser.add_argument("-c", "--config", type=str, nargs='*', help="Load corrections from the input files (space seperated). Default is to load the PTSUE file.", default=[DELAY_CONFIG])
    parser.add_argument("-v", action="store_true", help="Verbose mode")
    parser.add_argument("--force", action="store_true", help="Force applying corrections even if already applied (don't use this)")
    parser.add_argument("infiles", type=str, nargs='+', help="Input archivve files for correction.")
    args = parser.parse_args()

    #Parse arguments
    infiles=args.infiles
    force_against_sanity=args.force
    modify = args.modify
    outfile_name = args.output_name
    verbose = args.v
    ext = args.extension
    outdir = args.output_dir
    conffiles = args.config

    if len(infiles) > 1 and outfile_name is not None:
        print("Can't operate on multiple files with -o option")
        sys.exit(1)

    conffiles.sort()
    for file in conffiles:
        print("Using config file:",file)

    if len(conffiles)==0:
        print("ERROR: No config files found!")
        sys.exit(1)


    print("")


    # Compute the delay
    cur_correct=0
    for infile in infiles:
        outfile=None
        print("Reading from:",infile)
        #work out outfile name if reqired
        if (ext is None) and (outdir is None) and modify:
            outfile=infile
        else:
            if modify:
                print("")
                print("*****")
                print("Error, can't have -m in combination with -u or -e")
                print("*****")
                print("")
                sys.exit(1)
            if outfile_name is not None:
                outfile=outfile_name
            elif ext is not None:
                instem=infile[:infile.find(".")]
                outfile = f"{instem}.{ext}"
        if outdir is not None:
            if outfile is None:
                outfile = os.path.basename(infile)
            else:
                outfile = os.path.basename(outfile)
            outfile = os.path.join(outdir,outfile)
        if outfile is None:
            print("No output file given, use -m, -e or -o")
            sys.exit(1)



            # read in the fits header
        ifile=open(infile,"rb")
        ifile.seek(0,0)
        mainhdr = (readfitsheader(ifile))
        # Now look through the extensions for the history table.
        exthdr  = (readfitsheader(ifile))
        while (exthdr.get("EXTNAME").val.strip() != "'HISTORY '"):
            ifile.seek(exthdr.getextsize(),1)
            exthdr=(readfitsheader(ifile))

        # we now have the history table, so check if we have already fixed delays
        histhdr=exthdr
        history = history_class(histhdr,ifile)
        alread_fixed=0
        for row in history.entries:
            if row['PROC_CMD'].startswith("dlyfix"):
                print("DELAYS ALREADY alread_fixed")
                print(f"   on '{row['DATE_PRO']}'")
                print(f"   by '{row['PROC_CMD'].strip()}'")
                if force_against_sanity:
                    print("*** --force option forces us to apply delays again which is probably a bad idea ***")
                alread_fixed=1

        # Now look for the subint table...
        exthdr  = (readfitsheader(ifile))
        while (exthdr.get("EXTNAME").val.strip() != "'SUBINT  '"):
            ifile.seek(exthdr.getextsize(),1)
            exthdr=(readfitsheader(ifile))
        # try and compute the centre freq from the first subint...
        subinthdr = exthdr
        bintab = binarytable(subinthdr)
        subint = bintab.readrow(ifile)
        try:
            len(subint['DAT_FREQ'])
            fsum = sum(subint['DAT_FREQ'])
            freq = fsum/float(len(subint['DAT_FREQ']))
        except TypeError:
            freq = subint['DAT_FREQ']

        cur_delay=float(mainhdr.get("STT_OFFS").val)
        corrs=getcorrection(mainhdr,freq,history,conffiles,verbose)

        corr = sum(c['corr'] for c in corrs)
        new_delay = cur_delay - cur_correct + corr
        print("Correction is %g s,\n\t total delay is %s s"%(corr,new_delay))

        if alread_fixed==1 and not (force_against_sanity):
            print("No correction made as already fixed!")
            sys.exit(1)


        # Add the history comment line:
        oldsize=len(history.output())
        for c in corrs:
            row=copy.deepcopy(history.entries[-1])
            msg=re.sub("\s\s*"," ",c['msg'])
            row['PROC_CMD']="dlyfix (%g) %s"%(c['corr'],msg)
            if len(row['PROC_CMD']) > 80:
                row['PROC_CMD'] = row['PROC_CMD'][:79]
            row['DATE_PRO'] = str(datetime.datetime.now(datetime.timezone.utc))
            history.appendrow(row)
        if len(corrs) == 0:
            print("No corrections to apply to this file")

        newsize=len(history.output())
        print(oldsize,newsize)
        if modify and newsize != oldsize:
            print("ERROR can't add to history table without changing file size")
            print("Cannot use --modify in this case")
            sys.exit(3)
        mainhdr.get("STT_OFFS").val=("%17.17f "%new_delay).rjust(18)

        #Write the main header:

        print("Writing to:",outfile)

        if modify:
            ifile=ofile=open(infile,"rb+")
        else:
            ofile=open(outfile,"wb")
        ofile.seek(0,0)
        ofile.write(mainhdr.output())

        #Write the extention tables...
        #Move the the start of the ext tables in the input file..
        ifile.seek(0,0)
        # For some reason you need to do this twice
        readfitsheader(ifile)
        exthdr = readfitsheader(ifile)
        while exthdr is not None:
            if exthdr.get("EXTNAME").val.strip() == "'HISTORY '":
                if modify:
                    # we have to overwrite the header, so seek backwards
                    ofile.seek(-len(history.hdr.output()),1)
                else:
                    # we are not re-reading the history, so skip over
                    ifile.seek(exthdr.getextsize(),1)
                ofile.write(history.output())
            else:
                toread=exthdr.getextsize()
                if modify:
                    ifile.seek(toread,1)
                else:
                    ofile.write(exthdr.output())
                    while toread > 0:
                        raw = ifile.read(2880)
                        ofile.write(raw)
                        toread -= 2880
            exthdr=readfitsheader(ifile)


        ofile.close()
        ifile.close()
        print("")


if __name__ == '__main__':
    main()
