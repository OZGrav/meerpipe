import argparse

from meerpipe.calc_max_nsub import calc_max_nsub

def main():
    parser = argparse.ArgumentParser(description="Calculate maximum number of time subintegratons of sensitive ToAs for an archive")
    parser.add_argument(
        "--sn",
        type=float,
        required=True,
        help="The signal-to-noise ratio of the archive",
    )
    parser.add_argument(
        "--nchan",
        type=int,
        required=True,
        help="The number of frequency channels in the decimated archive",
    )
    parser.add_argument(
        "--duration",
        type=float,
        required=True,
        help="The duration of the archive in seconds",
    )
    parser.add_argument(
        "--input_nsub",
        type=float,
        required=True,
        help="The number of subintegrations of the input archive",
    )
    parser.add_argument(
        "--sn_desired",
        type=float,
        default=12.,
        help="The desired signal-to-noise ratio (default: 12.)",
    )
    parser.add_argument(
        "--minimum_duration",
        type=float,
        default=480.,
        help="The minimum duration of the archive in seconds (default: 480.)",
    )
    args = parser.parse_args()

    nsub = calc_max_nsub(
        args.sn,
        args.nchan,
        args.duration,
        args.input_nsub,
        sn_desired=args.sn_desired,
        minimum_duration=args.minimum_duration,
    )
    if nsub <= 1:
        print("1")
    else:
        print(f"1 {nsub}")


if __name__ == '__main__':
    main()