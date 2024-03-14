import os
import logging

import psrchive as ps

from meerpipe.utils import setup_logging
from meerpipe.archive_utils import chopping_utility

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')

logger = setup_logging(console=True, level=logging.DEBUG)

def test_chopping_utility():
    # Test archives for each bands that have been time and polarisation scrunched.
    test_archives = [
        # Standard 1024 nchan
        ("UHF", "J0255-5304_2020-08-03-23:36:45_zap.ar", 1024, 928),
        ("LBAND", "J0437-4715_2019-03-26-16:26:02_zap.ar", 1024, 928),
        ("SBAND_0", "J0737-3039A_2023-05-16-11:38:55_zap_ch1024.ar", 1024, 928),
        ("SBAND_1", "J1757-1854_2023-12-03-09:37:38_zap.ar", 1024, 928),
        # ("SBAND_2", "", 1024, 928),
        # ("SBAND_3", "", 1024, 928),
        ("SBAND_4", "J1811-1736_2023-11-16-14:54:16_zap.ar", 1024, 928),
        # Old obs that has 928 nchan
        ("LBAND", "J1644-4559_2019-08-07-15:41:45_zap.ar", 928, 928),
        # A 768 nchan obs
        ("LBAND", "J1827-0750_2020-01-10-08:29:29_zap.ar", 768, 768),
        # High nchan obs
        ("UHF", "J1350-5115_2022-01-03-04:04:15_zap.ar", 4096, 3712),
        ("LBAND", "J2317+1439_2022-12-03-15:09:15_zap.ar", 4096, 3712),
        ("LBAND", "J0737-3039A_2022-09-18-07:10:50_zap.ar", 16384, 14848),
        ("SBAND_0", "J0737-3039A_2023-05-16-11:38:55_zap_ch16384.ar", 16384, 14848),
        ("SBAND_1", "J1756-2251_2023-05-25-20:54:23_zap.ar", 16384, 14848),
    ]
    for band, archive, input_nchan, output_nchan in test_archives:
        logger.info(f"Testing chopping utility for {archive} in {band} band")
        # Initialize the archive
        ar = ps.Archive_load(os.path.join(TEST_DATA_DIR, archive))
        assert ar.get_nchan() == input_nchan

        # Chop the archive
        chopping_utility(os.path.join(TEST_DATA_DIR, archive), band, logger=logger)

        # Check the nchan of the chopped archive
        chopped_archive = archive.replace(".ar", "_chopped.ar")
        ar = ps.Archive_load(os.path.join(TEST_DATA_DIR, chopped_archive))
        logger.info(f"Testing {ar.get_nchan()} == {output_nchan}")
        assert ar.get_nchan() == output_nchan

        # Remove the chopped archive
        os.remove(os.path.join(TEST_DATA_DIR, chopped_archive))