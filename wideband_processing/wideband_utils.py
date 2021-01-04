#!/usr/bin/env python

import os
import sys
import shlex
import subprocess
import argparse
import os.path
import numpy as np
import logging
import glob
from shutil import copyfile


def setup_logging(path,verbose,file_log):
    """
    Setup log handler - this logs in the terminal

    """
    log_toggle=False

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if file_log == True:
        logfile = "wideband.log"
        logger = logging.getLogger(logfile)
        logger.setLevel(logging.INFO)

        if not os.path.exists(path):
            os.makedirs(path)
        #Create file logging only if logging file path is specified
        fh = logging.FileHandler(os.path.join(path,logfile))
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        #Check if file already exists, if so, add a demarcator to differentiate among runs
        if os.path.exists(os.path.join(path, logfile)):
            with open(os.path.join(path,logfile), 'a') as f:
                f.write(20*"#")
                f.write("\n")
        logger.info("File handler created")
        log_toggle=True

    if verbose:
        #Create console handler with a lower log level (INFO)
        logfile = "wideband.log"
        logger = logging.getLogger(logfile)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(ch)
        logger.info("Verbose mode enabled")
        log_toggle=True

    if log_toggle:
        return logger
    else:
        return none
