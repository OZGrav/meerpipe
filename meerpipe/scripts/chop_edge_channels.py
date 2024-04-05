#!/usr/bin/env python

import argparse

from meerpipe.archive_utils import chopping_utility


def main():
    parser = argparse.ArgumentParser(description="Chop the edge frequency channels of a meertime archive")
    parser.add_argument("archive_path", help="Cleaned (psradded) archive.")
    parser.add_argument("--band", help="The frequency band of the archive.")
    args = parser.parse_args()

    chopping_utility(args.archive_path, args.band)


if __name__ == '__main__':
    main()
