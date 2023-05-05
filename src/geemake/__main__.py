import os
import sys
import subprocess

import ee
import geemap

geemap.ee_initialize()

from geemake import utils


def main():
    """ Run Snakemake with Google Earth Engine Assets.

    First checks if any of the Earth Engine assets used in the Snakemake
    workflow have been updated (telling Snakemake if they have) then runs
    Snakemake (passing all command line arguments on to Snakemake)

    Args:
         None

    Returns:
        None
    """
    try:
        summary = subprocess.run(
            ["snakemake", "--summary"],
            check=True,
            text=True,
            capture_output=True
        )
    except subprocess.CalledProcessError as E:
        print('Error in snakefile. Try running `snakemake --summary`')
        return

    # split summary into list of each line
    # dropping the first line as it only has column headers
    summary = summary.stdout.split('\n')[1:]
    files = [x.split('\t')[0] for x in summary]

    for f in files:
        try:
            asset = utils.check_update_time(f)
        except ee.EEException:
            asset = None
        if os.path.isfile(f):
            os.remove(f)
        if asset is not None:
            utils.write_update_time(asset, f)

    subprocess.run(["snakemake", "-c1"] + sys.argv[1:])
