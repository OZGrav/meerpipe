import os
import json
import shlex
import subprocess
import numpy as np
import argparse

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#psrchive imports
import psrchive as ps

from coast_guard import clean_utils

#Importing scintools (@dreardon)
from scintools.dynspec import Dynspec

from meerpipe.utils import setup_logging
from meerpipe.archive_utils import template_adjuster, calc_dynspec_zap_fraction


def generate_SNR_images(
        scrunched_file,
        label,
        logger=None,
    ):
    # Load logger if no provided
    if logger is None:
        logger = setup_logging(console=True)

    logger.info("----------------------------------------------")
    logger.info(f"Creating {label} S/N images...")
    logger.info("----------------------------------------------")

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

    try:
        dyn = Dynspec(dynspec_file, process=False, verbose=False)
        dyn.plot_dyn(filename=f"{dynspec_file}.png" ,display=False, title=f"Dynamic Spectral ({label})")
        logger.info("Refilling")
        dyn.trim_edges()
        dyn.refill(linear=False)
    except Exception as e:
        logger.error("Scintools failed. Dyanmic spectra couldn't be created do to :")
        logger.error(e)


def generate_images(
        pid,
        raw_file,
        clean_file,
        raw_scrunched,
        clean_scrunched,
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
    generate_SNR_images(raw_scrunched,   'raw',     logger=logger)
    if clean_scrunched:
        generate_SNR_images(clean_scrunched, 'cleaned', logger=logger)



    logger.info("----------------------------------------------")
    logger.info("Generating dynamic spectra using psrflux")
    logger.info("----------------------------------------------")

    if clean_file:
        generate_dynamicspec_images(raw_file,   template, 'raw',     logger=logger)
        generate_dynamicspec_images(clean_file, template, 'cleaned', logger=logger)

def generate_results(
        snr,
        dm_file,
        cleaned_FTp_file,
        dynspec_file,
        logger=None,
    ):
    # Load logger if no provided
    if logger is None:
        logger = setup_logging(console=True)

    logger.info("----------------------------------------------")
    logger.info("Generating results.json")
    logger.info("----------------------------------------------")

    # Results dict that will be turned into a json
    results = {}

    # Calculate the RFI fraction
    logger.info("Calculating RFI fraction")
    rfi_frac = float(calc_dynspec_zap_fraction(dynspec_file))
    results["percent_rfi_zapped"] = rfi_frac

    # Read in DM values
    logger.info("Reading in DM values")
    with open(dm_file, "r") as f:
        lines = f.readlines()
        dm = lines[0].split()[-1]
        if dm == "None":
            results["dm"] = None
        else:
            results["dm"] = float(dm)

        dm_err = lines[1].split()[-1]
        if dm_err == "None":
            results["dm_err"] = None
        else:
            results["dm_err"] = float(dm_err)

        dm_epoch = lines[2].split()[-1]
        if dm_epoch == "None":
            results["dm_epoch"] = None
        else:
            results["dm_epoch"] = float(dm_epoch)

        dm_chi2r = lines[3].split()[-1]
        if dm_chi2r == "None":
            results["dm_chi2r"] = None
        else:
            results["dm_chi2r"] = float(dm_chi2r)

        dm_tres = lines[4].split()[-1]
        if dm_tres == "None":
            results["dm_tres"] = None
        else:
            results["dm_tres"] = float(dm_chi2r)

        rm = lines[5].split()[-1]
        if rm == "None" or rm == "RM:":
            results["rm"] = None
        else:
            results["rm"] = float(rm)

        rm_err = lines[6].split()[-1]
        if rm_err == "None" or rm_err == "RM_ERR:":
            results["rm_err"] = None
        else:
            results["rm_err"] = float(rm_err)

    # Add input SNR value
    results["sn"] = float(snr)

    # Calculate flux
    comm = f"pdv -f {cleaned_FTp_file}"
    logger.info("Running flux calc command:")
    logger.info(comm)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    info = proc.stdout.read().decode("utf-8")
    logger.info(f"pdv output: {info}")
    flux = info.split("\n")[1].split()[6]
    results["flux"] = float(flux)

    # TODO calculate RM

    # Dump results to json
    with open("results.json", "w") as f:
        json.dump(results, f, indent=1)

    return

def main():
    parser = argparse.ArgumentParser(description="Flux calibrate MTime data")
    parser.add_argument("-pid", dest="pid", help="Project id (e.g. PTA)", required=True)
    parser.add_argument("-rawfile", dest="rawfile", help="Raw (psradded) archive", required=True)
    parser.add_argument("-cleanedfile", dest="cleanedfile", help="Cleaned (psradded) archive")
    parser.add_argument("-rawFp", dest="rawFp", help="Frequency, time  and polarisation scrunched raw archive", required=True)
    parser.add_argument("-cleanFp", dest="cleanFp", help="Frequency and polarisation scrunched cleaned archive")
    parser.add_argument("-cleanFTp", dest="cleanFTp", help="Frequency, time and polarisation scrunched cleaned archive")
    parser.add_argument("-template", dest="template", help="Path to par file for pulsar")
    parser.add_argument("-parfile", dest="parfile", help="Path to par file for pulsar", required=True)
    parser.add_argument("-rcvr", dest="rcvr", help="Bandwidth label of the receiver (LBAND, UHF)", required=True)
    parser.add_argument("-snr", dest="snr", help="Signal to noise ratio of the cleaned profile")
    parser.add_argument("-dmfile", dest="dmfile", help="The text file with the SM results")
    args = parser.parse_args()

    logger = setup_logging(console=True)

    generate_images(
        args.pid,
        args.rawfile,
        args.cleanedfile,
        args.rawFp,
        args.cleanFp,
        args.template,
        args.parfile,
        rcvr="LBAND",
        logger=logger,
    )

    if args.cleanedfile:
        # Dynamic spectrum file will be created in generate_images
        dynspec_file = f"{args.cleanedfile}.dynspec"

        generate_results(
            args.snr,
            args.dmfile,
            args.cleanFTp,
            dynspec_file,
            logger=logger,
        )


if __name__ == '__main__':
    main()