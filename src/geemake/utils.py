from datetime import datetime

import ee
import geemap

geemap.ee_initialize()


def epoch_time(timestamp):
    """ Convert Earth Engine asset updateTime timestampt to epoch time.

    Adapted from https://stackoverflow.com/a/30476450

    Args:
        timestamp: string containing a time stamp

    Returns:
        float, the epoch time representation of the input timestamp
    """
    utc_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
    return epoch_time


def get_update_time(asset):
    """ Returns the update time of an EE asset in epoch time.

    Args:
        asset: string, path to an earth engine asset.

    Returns:
        float: the update time of the asset in epoch time.
    """
    return epoch_time(ee.data.getAsset(asset)['updateTime'])


def write_update_time(asset, local):
    """ Writes the path and update time of the ee asset to local file.

    Does nothing if the asset does not exist.

    Args:
        asset: str, path to an ee asset
        local: str, path to a local file used to track the ee asset

    Returns:
        None
    """
    try:
        update_time = get_update_time(asset)
    except ee.EEException:
        return

    with open(local, 'w') as f:
        f.write(asset)
        f.write('\n')
        f.write(str(update_time))


def check_update_time(local):
    """ Checks if ee asset updateTime matches the updateTime stored in local.

    Args:
        local: str, path to a local file used to track an ee asset

    Returns:
        asset:, str, path to the corresponding ee asset if local needs to be
        updated or None if the updateTime of the local file matches the asset
    """
    with open(local, 'r') as f:
        lines = f.readlines()
        asset = lines[0].strip()
        local_update_time = lines[1].strip()
        true_update_time = get_update_time(asset)

        if local_update_time != true_update_time:
            return asset
