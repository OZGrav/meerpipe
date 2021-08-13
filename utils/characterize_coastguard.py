#!/usr/bin/env python

#Import CoastGuard
from coast_guard import cleaners
import argparse
import logging
import psrchive as ps
import sys
import os
import subprocess
import shlex


parser=argparse.ArgumentParser(description="Run CG on input archive file")
parser.add_argument("-ar",dest="archivepath", help="Path to the archive file")
parser.add_argument("-temp",dest="templatepath", help="Path to the template file")
parser.add_argument("-ct", dest="chan_thresh", help="Channel threshold")
parser.add_argument("-st", dest="subint_thresh", help="Subint threshold")
parser.add_argument("-out", dest="output_path", help="Output path (for custom CG)")
args=parser.parse_args()

archive = str(args.archivepath)
template = str(args.templatepath)

if not args.chan_thresh and not args.subint_thresh:

    subint_thresholds = [3,5,7,9,11]
    chan_thresholds = [3,5,7,9,11]

    for chanthresh in subint_thresholds:
        for subintthresh in chan_thresholds:


            #Load an Archive file
            loaded_archive = ps.Archive_load(archive)
            archive_path,archive_name = os.path.split(loaded_archive.get_filename())
            archive_name_orig = archive_name.split('.')[0]

            #Renaming archive file with statistical thresholds
            archive_name = archive_name_orig+'_zap_ch{0}_sub{1}.ar'.format(chanthresh,subintthresh)
            
            output_path = "/fred/oz005/users/aparthas/coastguard_characterize/2019-07-14-16:18:08/"
            print (archive_name)

            if not os.path.exists(os.path.join(output_path,archive_name)):
            
                #Use the various cleaners
                #Surgical cleaner
                print ("Applying the surgical cleaner")
                surgical_cleaner = cleaners.load_cleaner('surgical')
                surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={1},subintthresh={2},template={0}'.format(str(args.templatepath),chanthresh,subintthresh)
                surgical_cleaner.parse_config_string(surgical_parameters)
                surgical_cleaner.run(loaded_archive)

                #RcvrStandard cleaner
                print ("Applying rcvrstd cleaner")
                rcvrstd_cleaner = cleaners.load_cleaner('rcvrstd')

                rcvrstd_parameters = 'badfreqs=None,badsubints=None,trimbw=0,trimfrac=0,trimnum=0,response=None'
                rcvrstd_cleaner.parse_config_string(rcvrstd_parameters)
                rcvrstd_cleaner.run(loaded_archive)

                #Bandwagon cleaner
                print ("Applying the bandwagon cleaner")
                bandwagon_cleaner = cleaners.load_cleaner('bandwagon')
                bandwagon_parameters = 'badchantol=0.99,badsubtol=1.0'
                bandwagon_cleaner.parse_config_string(bandwagon_parameters)
                bandwagon_cleaner.run(loaded_archive)

                #Unload the Archive file
                print ("Unloading the cleaned archive")
                loaded_archive.unload("{0}/{1}".format(output_path,archive_name))

            else:
                print ("{0} name exists".format(archive_name))

else:
            chan_thresh = float(args.chan_thresh)
            subint_thresh = float(args.subint_thresh)


            #Load an Archive file
            loaded_archive = ps.Archive_load(archive)
            archive_path,archive_name = os.path.split(loaded_archive.get_filename())
            archive_name_orig = archive_name.split('.')[0]
            psrname = archive_name_orig.split('_')[0]

            #Renaming archive file with statistical thresholds
            archive_name = archive_name_orig+'_ch{0}_sub{1}.ar'.format(chan_thresh,subint_thresh)
            
            output_path = str(args.output_path)
            print (archive_name)

            if not os.path.exists(os.path.join(output_path,archive_name)):
            
                #Use the various cleaners
                #Surgical cleaner
                print ("Applying the surgical cleaner")
                surgical_cleaner = cleaners.load_cleaner('surgical')
                surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={1},subintthresh={2},template={0}'.format(str(args.templatepath),chan_thresh,subint_thresh)
                surgical_cleaner.parse_config_string(surgical_parameters)
                surgical_cleaner.run(loaded_archive)

                #RcvrStandard cleaner
                print ("Applying rcvrstd cleaner")
                rcvrstd_cleaner = cleaners.load_cleaner('rcvrstd')

                rcvrstd_parameters = 'badfreqs=None,badsubints=None,trimbw=0,trimfrac=0,trimnum=0,response=None'
                rcvrstd_cleaner.parse_config_string(rcvrstd_parameters)
                rcvrstd_cleaner.run(loaded_archive)

                #Bandwagon cleaner
                print ("Applying the bandwagon cleaner")
                bandwagon_cleaner = cleaners.load_cleaner('bandwagon')
                bandwagon_parameters = 'badchantol=0.99,badsubtol=1.0'
                bandwagon_cleaner.parse_config_string(bandwagon_parameters)
                bandwagon_cleaner.run(loaded_archive)

                #Unload the Archive file
                print ("Unloading the cleaned archive")
                loaded_archive.unload("{0}/{1}".format(output_path,archive_name))

                if os.path.exists("{0}/{1}".format(output_path,archive_name)):
                    snr = 'psrstat -jFTpD -c snr=pdmp,snr {0}/{1} -Q -q'.format(output_path,archive_name)
                    args_snr = shlex.split(snr)
                    proc_snr = subprocess.Popen(args_snr, stdout=subprocess.PIPE)
                    snr = value = round(float(proc_snr.stdout.readline()),2)
                    
                    with open (os.path.join(output_path,'{0}_{1}_{2}_cgstats.txt'.format(
                        psrname,subint_thresh,chan_thresh)),'w') as f:
                        f.write("{0},{1},{2},{3},{4} \n".format(psrname,archive_name_orig,chan_thresh,subint_thresh,snr))
                    f.close()

                    os.remove("{0}/{1}".format(output_path,archive_name))

            else:
                print ("{0} name exists".format(archive_name))



