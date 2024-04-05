import math
import numpy as np

def calc_max_nsub(
        sn,
        nchan,
        duration,
        input_nsub,
        sn_desired=12.,
        minimum_duration=480.,
    ):
    """
    Calculate the maximum number of time subintegrations of sensitive ToAs for an archive.

    Parameters
    ----------
    sn : float
        The signal-to-noise ratio of the archive.
    nchan : int
        The number of frequency channels in the decimated archive.
    duration : float
        The duration of the archive in seconds.
    input_nsub : int
        The number of subintegrations of the input archive.
    sn_desired : float
        The desired signal-to-noise ratio (default: 12.).
    minimum_duration : float
        The minimum duration of the archive in seconds (default: 480.).

    Returns
    -------
    nsub : int
        The estimated number of subintegrations that will create the maximum number of sensitive ToAs.
    """
    # Calc estimated sn when channelised
    sn_chan = sn / np.sqrt(nchan)

    # Calc duration estimate to get desired sn
    estimated_duration = duration * ( sn_desired / sn_chan ) **2

    if estimated_duration < minimum_duration:
        # estimated duration is less than minimum so us minimum
        estimated_duration = minimum_duration

    # Work out nsub using math.floor to round down
    nsub = math.floor( duration / estimated_duration )

    if nsub > input_nsub:
        # nsub is greater than input so use input
        nsub = input_nsub

    return nsub
