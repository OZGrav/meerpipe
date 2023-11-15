"""
Code containing utilities for parsing/processing pulsar archive files

__author__ = "Aditya Parthasarathy, Andrew Cameron"
__copyright__ = "Copyright (C) 2023 Aditya Parthasarathy, Andrew Cameron"
__license__ = "Public Domain"
__version__ = "0.1"
"""

#Basic imports
import os
import numpy as np

#Importing scintools (@dreardon)
from scintools.dynspec import Dynspec

#psrchive imports
import psrchive as ps


def get_band(bw, freq):
    """
    Determine which observing band is in use

    Parameters
    ----------
    bw: str
        The bandwidth of the observation in MHz
    freq: float
        The observing frequency in MHz

    Returns
    -------
    band: str
        The observing band name (LBAND, UFH, SBAND_0, SBAND_1, SBAND_2, SBAND_3, SBAND_4)
    """
    # 18/05/2023 - now expanding this with S-Band functionality
    # Specifications per Vivek
    # BAND - FREQ (MHz) - BW (MHz)
    # S0   - 2187.50    - 1750.00 - 2625.00 (875)
    # S1   - 2406.25    - 1968.75 - 2843.75 (875)
    # S2   - 2625.00    - 2187.50 - 3062.50 (875)
    # S3   - 2843.75    - 2406.25 - 3281.25 (875)
    # S4   - 3062.50    - 2625.00 - 3500.00 (875)

    if (bw == "544.0") and (freq < 816) and (freq > 815):
        band = "UHF"
    elif (freq < 1284) and (freq > 1283):
        band = "LBAND"
    elif (bw == "875.0") and (freq < 2189) and (freq > 2185):
        band = "SBAND_0"
    elif (bw == "875.0") and (freq < 2408) and (freq > 2404):
        band = "SBAND_1"
    elif (bw == "875.0") and (freq < 2627) and (freq > 2623):
        band = "SBAND_2"
    elif (bw == "875.0") and (freq < 2845) and (freq > 2841):
        band = "SBAND_3"
    elif (bw == "875.0") and (freq < 3064) and (freq > 3060):
        band = "SBAND_4"
    else:
        band = None

    return band



# Utility function - adjusts a template to match the requirements of RFI mitigation
# This includes:
# - matching the phase bins of the provided file, if possible
# - de-dedispersing the template, if required
# returns a copy of the template which can be safely deleted as needed
def template_adjuster(template, archive, output_dir, logger):

    # setup
    template_ar = ps.Archive_load(str(template))
    template_bins = int(template_ar.get_nbin())
    archive_bins = int(ps.Archive_load(str(archive)).get_nbin())

    # NEW 10/02/2023 - check for dedispersion and channel count
    if (template_ar.get_dedispersed() and template_ar.get_nchan() == 1):

        # convert the archive to undo dedispersion (vap -c dmc == 0)
        logger.info("De-dedispersing the temporary template...")
        template_ar.set_dedispersed(False)
        logger.info("Template successfully de-dedispersed.")

    # if bin counts don't match
    if not (template_bins == archive_bins):

        logger.info("Mismatch detected between phase bin count of archive ({0}) and template ({1})".format(archive_bins, template_bins))
        logger.info("Attempting to produce scrunched template to correct for mismatch...")

        # work out the mismatch factor
        b_factor = template_bins / archive_bins
        b_remainder = template_bins % archive_bins

        if not (b_remainder == 0):
            logger.info("Non-integer factor between the bin values, cannot scrunch to match - skipping step")
        elif (b_factor < 1.0):
            logger.info("Archive has higher bin count than template - skipping step")
        else:
            # create bin-scrunched clone and write to temporary file
            logger.info("Creating scrunched template by factor {}".format(b_factor))
            template_ar.bscrunch_to_nbin(archive_bins)

    # the scrunch has now either been done or it has not
    # write out the temporary standard
    new_template = os.path.join(str(output_dir),"temporary_{}.std".format(archive_bins))
    template_ar.unload(new_template)

    return new_template


# calculate the zapped fraction of a file based on the dynspec file
def calc_dynspec_zap_fraction(dynspec_file):

    if (os.path.isfile(dynspec_file)):
        # convert the dynspec file into a numpy array
        data = np.loadtxt(dynspec_file, comments='#')

        # loop through and count
        zap_lines = 0
        for x in range(0, len(data)):

            # check for zap condition
            if (float(data[x][4]) == 0) and (float(data[x][5]) == 0):
                zap_lines = zap_lines + 1

        retval = float(zap_lines)/float(len(data))

    else:
        raise Exception ("File {0} cannot be found".format(dynspec_file))

    return retval
