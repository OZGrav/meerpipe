import os
import sys
import shlex
import subprocess
import numpy as np
from glob import glob
import argparse

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#psrchive imports
import psrchive as ps

from coast_guard import clean_utils

#Importing scintools (@dreardon)
from scintools.dynspec import Dynspec

from meerpipe.initialize import setup_logging
from meerpipe.meerwatch_tools import plot_toas_fromarr
from meerpipe.archive_utils import template_adjuster


def generate_SNR_images(
        archive_file,
        label,
        logger=None,
    ):
    # Load logger if no provided
    if logger is None:
        logger = setup_logging(console=True)

    logger.info("----------------------------------------------")
    logger.info(f"Creating {label} S/N images...")
    logger.info("----------------------------------------------")

    # make scrunched file for analysis
    comm = f"pam -Fp -e Fp.temp {archive_file}"
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    info = proc.stdout.read().decode("utf-8").rstrip().split()
    scrunched_file = info[0]

    # new - psrchive side functionality
    scrunched_arch = ps.Archive_load(scrunched_file)
    zapped_arch = scrunched_arch.clone()

    # get parameters for looping
    comm = f"vap -c nsub,length {scrunched_file}"
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    info = proc.stdout.read().decode("utf-8").rstrip().split("\n")
    nsub = int(info[1].split()[1])
    length = float(info[1].split()[2])

    logger.info("Beginning S/N analysis...")
    logger.info("NSUB = {0} | LENGTH = {1}".format(nsub, length))

    # collect and write snr data
    snr_data = []
    for x in range(0, nsub):
        # step 1. work backward through the file zapping out one subint at a time
        asub = nsub - x - 1
        if (x > 0):
            # don't need to zap anything - analysing the complete archive
            clean_utils.zero_weight_subint(zapped_arch, asub + 1)

        # step 2. scrunch and write to disk
        tscr_arch = zapped_arch.clone()
        tscr_arch.tscrunch()
        temp_file = "zaptemp.ar"
        tscr_arch.unload(temp_file)

        # step 3. extract the cumulative snr via psrstat
        comm = f"psrstat -j Fp -c snr=pdmp -c snr {temp_file}"
        args = shlex.split(comm)
        proc = subprocess.Popen(args,stdout=subprocess.PIPE)
        proc.wait()
        snr_cumulative = float(proc.stdout.read().decode("utf-8").rstrip().split("=")[1])

        # step 4. extract the single snr via psrstat
        comm = f"psrstat -j Fp -c snr=pdmp -c subint={asub} -c snr {scrunched_file}"
        args = shlex.split(comm)
        proc = subprocess.Popen(args,stdout=subprocess.PIPE)
        proc.wait()
        snr_single = float(proc.stdout.read().decode("utf-8").rstrip().split("=")[1])

        # step 5. write to file
        #snr_data.append([length*x/nsub, snr_single, snr_cumulative])
        snr_data.append([length*asub/nsub, snr_single, snr_cumulative])

        # cleanup
        os.remove(temp_file)
        del(tscr_arch)

        #logger.info("Loop {} ending...".format(x))

    np.savetxt(f"{label}_snr.dat", snr_data, header=" Time (seconds) | snr (single) | snr (cumulative)", comments="#")

    logger.info("Analysis complete.")

    # plot results - single subint snr
    matplot_commands = [
        {
            'x-axis': np.transpose(snr_data)[0],
            'y-axis': np.transpose(snr_data)[1],
            'xlabel': 'Time (seconds)',
            'ylabel': 'SNR',
            'title': 'Single subint SNR ({0})',
            'name': 'SNR_single',
            'rank': 7,
            'type': 'snr-single.hi',
        },
        {
            'x-axis': np.transpose(snr_data)[0],
            'y-axis': np.transpose(snr_data)[2],
            'xlabel': 'Time (seconds)',
            'ylabel': 'SNR',
            'title': 'Cumulative SNR ({0})',
            'name': 'SNR_cumulative',
            'rank': 6,
            'type': 'snr-cumul.hi',
        },
    ]

    for matplot_command in matplot_commands:

        logger.info("Creating image type {0}...".format(matplot_command['type']))

        # create the plot
        image_name = f"{label}_{matplot_command['name']}.png"
        plt.clf()
        plt.plot(  matplot_command['x-axis'], matplot_command['y-axis'])
        plt.xlabel(matplot_command['xlabel'])
        plt.ylabel(matplot_command['ylabel'])
        plt.title( matplot_command['title'].format(label))
        plt.savefig(image_name)
        plt.clf()

    # cleanup
    os.remove(scrunched_file)

    return {'file': image_name, 'rank': matplot_command['rank'], 'type': matplot_command['type']}


def generate_dynamicspec_images(
        archive_file,
        template,
        label,
        logger=None,
    ):
    ar = ps.Archive_load(archive_file)

    # account for phase bin differences
    temporary_template = template_adjuster(template, archive_file, "./", logger)

    logger.info(f"Making dynamicspectra for {label} archive: {archive_file}")

    psrflux_com = f'psrflux -s {temporary_template} {archive_file} -e dynspec'

    proc_psrflux = shlex.split(psrflux_com)
    p_psrflux = subprocess.Popen(proc_psrflux)
    p_psrflux.wait()

    # Work out what name of output psrflux file is
    dynspec_file = f"{archive_file}.dynspec"


    # new_name = os.path.join(ds_path,"{0}".format(dynspec_name))
    # logger.info("Old name:{0}".format(old_name))
    # logger.info("New name:{0}".format(new_name))
    # os.rename(old_name,new_name)
    # logger.info("Dynamic spectra generated and moved to Scintillation directory: {0}".format(dynspec_name))

    # logger.info("Creating dynamic spectra plots using scintools")
    # dynspec_file = glob.glob(os.path.join(ds_path,"{0}".format(dynspec_name)))[0]

    try:
        dyn = Dynspec(dynspec_file, process=False, verbose=False)
        dyn.plot_dyn(filename=f"{dynspec_file}.png" ,display=False, title=f"Dynamic Spectral ({label})")
        logger.info("Refilling")
        dyn.trim_edges()
        dyn.refill(linear=False)
        #logger.info("Secondary spectra")
        #dyn.cut_dyn(tcuts=0, fcuts=7, plot=True, filename=os.path.join(ds_path,"{0}_subband.png".format(archive_name)))

    except:
        logger.info("Scintools failed. Dyanmic spectra couldn't be created")


def generate_images(
        pid,
        raw_file,
        clean_file,
        residuals,
        template,
        ephemeris,
        rcvr="LBAND",
        logger=None,
    ):
    # Load logger if no provided
    if logger is None:
        logger = setup_logging(console=True)

    # Note - the functionality of this code is based on the outputs expected by 'generate_summary'
    # Should these expected outputs change, the conditions of this code should be re-assessed

    logger.info("Generating pipeline images")
    image_data = []

    # Generate SNR images for raw then cleaned file
    image_data.append(generate_SNR_images(raw_file,   'raw',     logger=logger))
    image_data.append(generate_SNR_images(clean_file, 'cleaned', logger=logger))



    logger.info("----------------------------------------------")
    logger.info("Generating TOA-based images")
    logger.info("----------------------------------------------")


    # logger.info("Producing single-obs image from modified MeerWatch residuals...")
    for residual in residuals:
        # get parameters from file name
        nchan = int(residual.split('ch')[0].split('t')[1])
        nsub  = int(residual.split('zap.')[1].split('t')[0])
        archive_extension = residual.split('.')[1]

        if nchan == 1 and nsub == 1:
            logger.info(f"Not processing {residual} because nchan == 1 and nsub == 1")
        else:
            logger.info(f"Processing {residual}")
            plot_toas_fromarr(residual, pid=pid, sequential=True, verb=True, rcvr=rcvr, nchan=nchan, out_file=f"toas_{archive_extension}.png")




    logger.info("----------------------------------------------")
    logger.info("Generating dynamic spectra using psrflux")
    logger.info("----------------------------------------------")

    generate_dynamicspec_images(raw_file,   template, 'raw',     logger=logger)
    generate_dynamicspec_images(clean_file, template, 'cleaned', logger=logger)

    # # calculate the RFI fraction
    # if cparams["db_flag"]:
    #     max_rfi_frac = float(calc_dynspec_zap_fraction(os.path.join(ds_path,"{0}".format(dynspec_name))))

    # # cleanup temporary template
    # logger.info("Cleaning up temporary template...")
    # os.remove(temporary_template)

    # # now for some tacked-on PSRDB stuff based on the highest RFI zap fraction
    # if cparams["db_flag"]:

    #     logger.info("PSRDB functionality activated - recording zapped RFI fraction based on dynamic spectra")

    #     # Create client
    #     db_client = GraphQLClient(cparams["db_url"], False)

    #     # we've already calculated the maximum RFI zap fraction - recall results field and update
    #     results = get_results(cparams["db_proc_id"], db_client, cparams["db_url"], cparams["db_token"])
    #     results['zap_frac'] = float(max_rfi_frac)
    #     update_id = update_processing(
    #         cparams["db_proc_id"],
    #         None,
    #         None,
    #         None,
    #         None,
    #         None,
    #         None,
    #         None,
    #         results,
    #         db_client,
    #         cparams["db_url"],
    #         cparams["db_token"]
    #     )
    #     if (update_id != cparams["db_proc_id"]) or (update_id == None):
    #         logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(cparams["db_proc_id"]))
    #     else:
    #         logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(cparams["db_proc_id"]))



    # logger.info("Adding dynamic spectra images found in {0}...".format(ds_path))

    # # look for two fixed dynspec images
    # dynspec_commands = [
    #     {
    #         'ext': 'zap.dynspec',
    #         'rank': 9,
    #         'type': '{0}.zap-dynspec.hi'.format(local_pid),
    #     },
    #     {
    #         'ext': 'calib.dynspec',
    #         'rank': 10,
    #         'type': '{0}.calib-dynspec.hi'.format(local_pid),
    #     }
    # ]

    # for dynspec_command in dynspec_commands:

    #     # check/recall image and store image_data
    #     data = glob.glob(os.path.join(ds_path, "*{0}.png".format(dynspec_command['ext'])))
    #     if (len(data) == 0):
    #         logger.error("No matches found in {0} for extension {1}".format(ds_path, dynspec_command['ext']))
    #     elif (len(data) > 1):
    #         logger.error("Non-unique match found in {0} for extension {1} - skipping".format(ds_path, dynspec_command['ext']))
    #     else:
    #         # unique match found
    #         logger.info("Unique match found in {0} for extension {1}".format(ds_path, dynspec_command['ext']))

    #         if (cparams["db_flag"]):

    #             # BUG FIX - We now need to check on the file size!
    #             max_image_size = 750 # kB
    #             dimension_factor = 0.95
    #             loop_counter = 0
    #             size_check = False

    #             logger.info("Checking on file size of {0} to determine if downsampling is needed for PSRDB upload...".format(data[0]))

    #             while not (size_check):

    #                 current_factor = dimension_factor**loop_counter

    #                 if (loop_counter == 0):
    #                     # initialise the image
    #                     og_image = Image.open(data[0])
    #                     og_sizes = og_image.size
    #                     data_split = os.path.splitext(data[0])
    #                     small_image_name = "{0}.small.jpg".format(data_split[0])
    #                     next_image_name = data[0]
    #                 else:
    #                     # make a downsized copy
    #                     small_image = og_image.convert('RGB')
    #                     small_image = small_image.resize((round(og_sizes[0]*current_factor), round(og_sizes[1]*current_factor)), Image.ANTIALIAS)
    #                     small_image.save(small_image_name, optimize=True, quality=95)
    #                     next_image_name = small_image_name

    #                 # image to be considered is ready - test file size (in KB)
    #                 image_size = os.stat(next_image_name).st_size / 1024
    #                 if (image_size <= max_image_size):
    #                     size_check = True
    #                     logger.info("Final image {2} downsampled {0} times ({1}% size of original)".format(loop_counter, current_factor*100, next_image_name))

    #                 loop_counter += 1

    #         else:
    #             next_image_name = data[0]

    #         image_data.append({'file': next_image_name, 'rank': dynspec_command['rank'], 'type': dynspec_command['type']})

    # # write all images to PSRDB
    # if (cparams["db_flag"]):


    #     logger.info("PSRDB functionality activated - recording pipeline images to PSRDB")

    #     # set up PSRDB functionality
    #     db_client = GraphQLClient(cparams["db_url"], False)

    #     for image_d in image_data:

    #         # test for image creation success and write to PSRDB
    #         if (os.path.exists(image_d['file'])):
    #             logger.info("Successfully created {0} - recording to PSRDB.".format(image_d['file']))
    #             create_pipelineimage(image_d['file'], image_d['type'], image_d['rank'], cparams, db_client, logger)
    #         else:
    #             logger.error("Unable to create {0} - no output recorded to PSRDB.".format(image_d['file']))

    # logger.info("Image generation & logging complete.")

        # now do DM measurement
    # logger.info("Initiating single-obs DM measurement (attached to TOA production)...")
    # dm_archive_name = "dm_toas.ar"
    # dm_archive_file = os.path.join(images_path, dm_archive_name)
    # dm_nchan = 16
    # dm_result = None

    # while (dm_result == None and dm_nchan >= 4 and (dm_nchan % 1 == 0)):

    #     if (build_dm_toas(output_dir, toa_file, dm_archive_name, images_path, dm_nchan, logger)):

    #         # dm archive successfully created
    #         # send archive to dm measurement function
    #         dm_result = measure_dm(dm_archive_file, images_path, ephemeris, template, selfile, logger)

    #         # output will either be a JSON object or None
    #     else:
    #         logger.error("Generation of {} channel DM archive was unsuccssful - dividing nchan by 2 and trying again.".format(dm_nchan))

    #     dm_nchan = dm_nchan / 2

    # # loop complete - report
    # if (dm_result == None):
    #     logger.info("DM fitting failed.")
    # else:
    #     logger.info("DM fitting succesful.")

    # # cleanup
    # if (os.path.exists(dm_archive_file)):
    #     os.remove(dm_archive_file)

    # # write results to PSRDB
    # if cparams["db_flag"]:

    #     logger.info("PSRDB functionality activated - recording measured DM.")

    #     # Create client
    #     db_client = GraphQLClient(cparams["db_url"], False)

    #     # recall results field and update
    #     psrdb_results = get_results(cparams["db_proc_id"], db_client, cparams["db_url"], cparams["db_token"])
    #     logger.info("Recalled results of processing ID {0}".format(cparams["db_proc_id"]))
    #     logger.info(psrdb_results)
    #     psrdb_results['dm'] = dm_result
    #     update_id = update_processing(
    #         cparams["db_proc_id"],
    #         None,
    #         None,
    #         None,
    #         None,
    #         None,
    #         None,
    #         None,
    #         psrdb_results,
    #         db_client,
    #         cparams["db_url"],
    #         cparams["db_token"]
    #     )
    #     if (update_id != cparams["db_proc_id"]) or (update_id == None):
    #         logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(cparams["db_proc_id"]))
    #     else:
    #         logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(cparams["db_proc_id"]))


    return

def main():
    parser = argparse.ArgumentParser(description="Flux calibrate MTime data")
    parser.add_argument("-pid", dest="pid", help="Project id (e.g. PTA)", required=True)
    parser.add_argument("-rawfile", dest="rawfile", help="Raw (psradded) archive", required=True)
    parser.add_argument("-cleanedfile", dest="cleanedfile", help="Cleaned (psradded) archive", required=True)
    parser.add_argument("-residuals", dest="residuals", help="TOA residuals file", required=True, nargs='*')
    parser.add_argument("-template", dest="template", help="Path to par file for pulsar", required=True)
    parser.add_argument("-parfile", dest="parfile", help="Path to par file for pulsar", required=True)
    parser.add_argument("-rcvr", dest="rcvr", help="Bandwidth label of the receiver (LBAND, UHF)", required=True)
    args = parser.parse_args()

    generate_images(
        args.pid,
        args.rawfile,
        args.cleanedfile,
        args.residuals,
        args.template,
        args.parfile,
        rcvr="LBAND",
        logger=None,
    )


if __name__ == '__main__':
    main()