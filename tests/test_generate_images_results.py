import os

from meerpipe.utils import setup_logging
from meerpipe.scripts.generate_images_results import dynamic_spectra

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')

logger = setup_logging(console=True)

def test_dynamic_spectra_image_size():
    test_files = [
        "J0737-3039A_2019-07-20-06:53:03_zap.ar.dynspec",
        "J0737-3039A_2019-08-28-03:19:36_zap.ar.dynspec",
        "J0737-3039A_2019-09-26-01:43:29_zap.ar.dynspec",
        "J1933-6211_2019-12-06-16:30:12_zap.ar.dynspec",
        "J0024-7204L_2019-11-05-21:15:44_zap.ar.dynspec",
        "J0955-6150_2019-12-01-09:05:04_zap.ar.dynspec",
    ]
    for test_file in test_files:
        dynamic_spectra_file = os.path.join(TEST_DATA_DIR, test_file)
        dynamic_spectra_image = os.path.join(TEST_DATA_DIR, f"{test_file}.png")
        dynamic_spectra(dynamic_spectra_file, "test", logger=logger)
        file_size_bytes = os.path.getsize(dynamic_spectra_image)
        print(f"File size: {file_size_bytes} bytes")
        os.remove(dynamic_spectra_image)

        # Check it is less than 1MB
        assert file_size_bytes < 1e6

