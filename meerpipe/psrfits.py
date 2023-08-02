#!/usr/bin/python
import fits,sys,struct


class history:
    def __init__(self,hdr,instream):
        self.read(hdr,instream)
        self.hdr=hdr

    def read(self,hdr,instream):
        self.entries=list()
        sz=hdr.getextsize()
        self.bintab = fits.binarytable(hdr)
        bytesread=0
        for row in range(0,self.bintab.nrow):
            raw=instream.read(self.bintab.rowsize)
            line = self.bintab.parserow(raw)
            self.entries.append(line)
            bytesread += self.bintab.rowsize

        skip=(2880-bytesread%2880)
        instream.seek(skip,1)

    def appendrow(self,row):
        self.entries.append(row)
        nrow=int(self.hdr.get("NAXIS2").val)
        nrow+=1
        self.hdr.get("NAXIS2").val=("%s "%nrow).rjust(18)
        row=int(self.hdr.get("NAXIS2").val)

    def output(self):
        out=self.hdr.output()
        for x in self.entries:
            out += self.bintab.writerow(x)
        size=len(out) + (2880-len(out)%2880)
        return out.ljust(size)
