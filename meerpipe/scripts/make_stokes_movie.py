import argparse
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import OrderedDict

import psrchive as ps


def grab_profile_data(archives, sn_min=20):
    profiles = []
    for archive in archives:
        arch = ps.Archive_load(archive)
        arch.remove_baseline()
        arch.convert_state(state="Stokes")
        prof = arch.get_Profile(0,0,0)

        sn = prof.snr()
        if sn > sn_min:
            # Rotate to a phase of 0.5 profile max
            rotate_by = prof.find_max_phase() - 0.5
            arch.rotate_phase(rotate_by)
            profiles.append(arch.get_data())
    return profiles


def normalise_profile(profile):
    profile = profile - min(profile)
    return profile / max(profile)


def make_profile_plot(profile_data):
    fig, ax = plt.subplots(figsize=(10,6))
    totals  = []
    linears = []
    circles = []
    for profile in profile_data:
        phase = list(range(profile.shape[3]))
        noramlise_by = max(profile[0][0][0][:])
        total_profile    = profile[0][0][0][:] / noramlise_by
        linear_profile   = np.sqrt(profile[0][1][0][:]**2 + profile[0][2][0][:]**2) / noramlise_by
        circular_profile = profile[0][3][0][:] / noramlise_by
        totals.append( ax.plot(phase, total_profile,    alpha=0.2, label='Total',    c="black")[0])
        linears.append(ax.plot(phase, linear_profile,   alpha=0.2, label='Linear',   c="red")[0])
        circles.append(ax.plot(phase, circular_profile, alpha=0.2, label='Circular', c="blue")[0])

    # Only plot the unique labels https://stackoverflow.com/questions/13588920/stop-matplotlib-repeating-labels-in-legend
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())

    plt.savefig("profile.png")

    # Make animation
    # https://matplotlib.org/stable/users/explain/animations/animations.html
    def update_profile_alpha(frame):
        # Make sure everything is the same alpha
        for total, linear, circle in zip(totals, linears, circles):
            total.set_alpha(0.2)
            linear.set_alpha(0.2)
            circle.set_alpha(0.2)
        # Put current frame alpha to 1
        totals[frame].set_alpha(1)
        linears[frame].set_alpha(1)
        circles[frame].set_alpha(1)

        return tuple(totals + linears + circles)
    ani = animation.FuncAnimation(fig=fig, func=update_profile_alpha, frames=len(profile_data), interval=500)
    ani.save(filename="profile.apng", writer="pillow")



def main():
    parser = argparse.ArgumentParser(description="Make a movie of all the polarisation profiles.")
    parser.add_argument("-a", "--archives", nargs="+", help="All of the archive files that you want to create a movie of.")
    parser.add_argument("-s", "--sn_min", help="Minium signal to noise ratio of archive to include in the movie.", default=20)
    args = parser.parse_args()

    profile_data = grab_profile_data(args.archives, sn_min=args.sn_min)
    make_profile_plot(profile_data)