
import sys
import struct

class fitsline:
    def __init__(self):
        self.val=None
        self.key=None
        self.comment=None
    def output(self):
        if self.key is None:
            return " ".ljust(80)
        if self.key=="END":
            return "END".ljust(80)
        if self.key=="COMMENT":
            return f"COMMENT{self.val}".ljust(80)
        if self.key=="HISTORY":
            return f"HISTORY{self.val}".ljust(80)
        x= "%s=% 22s"%(self.key.ljust(8),self.val)
        if self.comment!=None:
            x = f"{x}/ {self.comment}"
        return x.ljust(80)

    def isend(self):
        return self.key=="END"
    def isblank(self):
        return self.key is None
    def isvalid(self):
        return self.key not in [None, "END"]
    def blank(self):
        self.key=None
    def comment(self,comment):
        self.key="COMMENT"
        self.val=comment

class fitsheader:

    def __init__(self,fields):
        self.ordered = []
        self.indexed = {}
        for field in fields:
            self.addfield(field)

    def addfield(self,field):
        if field.isvalid():
            self.ordered.append(field)
            self.indexed[field.key]=field

    def get(self,key):
        return self.indexed[key] if key in list(self.indexed.keys()) else None

    def output(self):
        out=""
        for field in self.ordered:
            out+=field.output()
        out+="END".ljust(80)
        size=len(out)
        if (size % 2880)!=0:
            size=len(out) + (2880-len(out)%2880)
        return out.ljust(size).encode("UTF-8")

    def getextsize(self):
        sz=0
        if "XTENSION" in list(self.indexed.keys()):
            naxis=int(self.get("NAXIS").val.strip())
            sz=1
            for i in range(1,naxis+1):
                sz*=int(self.get("NAXIS%d"%i).val)
            if sz%2880 > 0:
                sz=sz + (2880-sz%2880)
        return sz



def readfitsheader(file):
    hdr = []
    idata=file.read(2880)
    if len(idata) < 2880:
        return None
    hdr.extend(parsefitshdr(idata))
    s=2880
    while hdr[-1].isvalid():
        idata=file.read(2880)
        hdr.extend(parsefitshdr(idata))
        s+=2880
    return fitsheader(hdr)


def parsefitshdr(hdr):
    if len(hdr) != 2880:
        print("ERROR header length not 2880 (%d)"%len(hdr))
        return 0
    ret = []
    ended=0
    for i in range(0,2880,80):
        line = hdr[i:i+80].decode("UTF-8")
        fl=fitsline()
        if ended:
            fl.key=None
            ret.append(fl)
            continue
        if line[:3]=="END":
            ended=1
            fl.key="END"
            ret.append(fl)
            continue
        if line[:7]=="COMMENT":
            key="COMMENT"
            val=line[7:]
            comment=""
        elif line[:7]=="HISTORY":
            key="HISTORY"
            val=line[7:]
            comment=""
        else:
            elems=line.split("=",1)
            if len(elems) < 2:
                print(f"BAD LINE with key '{elems[0]}'")
                continue
            key=elems[0].strip()
            elems=elems[1].split("/",1)
            val=elems[0]
            if len(elems) > 1:
                comment=elems[1].strip()
            else:
                comment=None
        fl.key=key
        fl.val=val
        fl.comment=comment
        ret.append(fl)
    return ret

class binarytable:
    def __init__(self, header):
        self.sorted = []
        self.indexed = {}
        self.rowsize=int(header.get("NAXIS1").val)
        self.nrow=int(header.get("NAXIS2").val)
        self.extver=int(header.get("EXTVER").val)
        self.tt=None
        i=1
        while 1:
            line = header.get("TTYPE%d"%i)
            if line is None:
                break;
            table_type = line.val.strip()[1:-1].strip()
            line = header.get("TFORM%d"%i)
            if line is None:
                break;
            fitsformat=line.val.strip()[1:-1].strip()
            n=fitsformat[:-1]
            F=fitsformat[-1]
            pyformat=None
            if F == "A":
                # string type
                pyformat = f"{n}s"
            elif F == "B":
                # 8-bit unsigned integer
                pyformat = "B" if n == "1" else f"{n}B"
            elif F == "D":
                # 64-bit precision floating point
                pyformat = "d" if n == "1" else f"{n}d"
            elif F == "E":
                # 32-bit precision floating point...
                pyformat = "f" if n == "1" else f"{n}f"
            elif F == "I":
                # 16-bit signed integer
                pyformat = "h" if n == "1" else f"{n}h"
            elif F == "J":
                # 32-bit signed integer
                pyformat = "i" if n == "1" else f"{n}i"
            elif F == "K":
                # 64-bit signed integer
                pyformat = "q" if n == "1" else f"{n}q"
            elif F == "X":
                # 1-bit value
                # For sanity, we read as n/8 bytes
                pyformat = "B" if n == "1" else "%dB"%(int(n)/8.0)
            if pyformat is None:
                print(f"ERROR: FITS format '{fitsformat}' not understood")
                sys.exit(1)

            elem = (table_type,fitsformat,pyformat)
            self.sorted.append(elem)
            self.indexed[table_type] = elem
            i+=1
        self.parsestring=">"
        for table_type,ffmt,pyfmt in self.sorted:
            self.parsestring+=pyfmt


    def readrow(self,file):
        return self.parserow(file.read(self.rowsize))

    def parserow(self,bytes):
        if len(bytes) != self.rowsize:
            return None
        ret = {}
        i=0
        elems=struct.unpack(self.parsestring,bytes)
        for row_type,ffmt,pyfmt in self.sorted:
            if pyfmt[-1] == "s":
                ret[row_type] = elems[i].decode("UTF-8")
                i+=1
            elif len(pyfmt) == 1:
                ret[row_type] = elems[i]
                i+=1
            else:
                # we have an array to read!
                ret[row_type] = []
                size = int(pyfmt[:-1])
                ret[row_type].extend(elems[i:i+size])
                i+=size
        return ret

    def writerow(self,row):
        bytes_row="".encode("UTF-8")
        for row_type,ffmt,pyfmt in self.sorted:
            val=row[row_type]
            if pyfmt[-1] == "s":
                row_str = struct.pack(f">{pyfmt}", val.encode("UTF-8"))
            elif len(pyfmt) == 1:
                row_str = struct.pack(f">{pyfmt}", val)
            else:
                row_str=""
                j=0
                while j < len(val):
                    row_str += struct.pack(f">{pyfmt[-1]}", val[j])
                    j+=1
            bytes_row+=row_str
        return bytes_row




class history_class:
    def __init__(self,hdr,instream):
        self.read(hdr,instream)
        self.hdr=hdr

    def read(self,hdr,instream):
        self.entries = []
        sz=hdr.getextsize()
        self.bintab = binarytable(hdr)
        bytesread=0
        for _ in range(self.bintab.nrow):
            raw=instream.read(self.bintab.rowsize)
            line = self.bintab.parserow(raw)
            self.entries.append(line)
            bytesread += self.bintab.rowsize

        skip=(2880-bytesread%2880)
        instream.seek(skip,1)

    def appendrow(self,row):
        self.entries.append(row)
        nrow = int(self.hdr.get("NAXIS2").val) + 1
        self.hdr.get("NAXIS2").val = f"{nrow} ".rjust(18)
        row=int(self.hdr.get("NAXIS2").val)

    def output(self):
        out=self.hdr.output()
        for x in self.entries:
            out += self.bintab.writerow(x)
        size=len(out) + (2880-len(out)%2880)
        return out.ljust(size)
