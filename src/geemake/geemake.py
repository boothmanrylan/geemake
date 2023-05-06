""" Allows Snake Make workflows to work with Earth Engine Assets.
"""
import os
import argparse
import time
import functools
from multiprocessing import Process

import ee
import geemap

geemap.ee_initialize()

from geemake import utils

def _poll(asset, local, task, wait=10):
    """ Start task, if it completes succesfully, write time to filename.

    Args:
        asset: str, path to the earth asset being created by task
        local: str, path to a local file used by snake make to track the asset
        task: Earth Engine task (e.g. from ee.batch.Export.*), creating the
            asset. Should not be started yet.
        wait: int, number of seconds to wait between checking if the task has
            completed, set this to be longer for tasks you know will take a
            long time to run, can be set to zero to skip waiting for very short
            running tasks

    Returns:
        str, the final status of the task (e.g. COMPLETED if the task was
        successful)
    """
    task.start()

    status = None

    while True:
        time.sleep(wait)

        status = task.status()['state']
        if status == 'READY' or status == 'RUNNING':
            continue
        elif status == 'COMPLETED':
            utils.write_update_time(asset, local)
            break
        else:
            break

    return status


def geemake(wait=10):
    """ Decorator to allow snakemake to work with Google Earth Engine.

    Converts the given function to read its arguments from the command line and
    track the status of the tasks it creates, writing out their time of
    succesful completion to local files so that they can be tracked by Snake
    Make.

    The function being decorated must take ee_input, ee_output, and
    local_output named arguments, must return a list of 3 item tuples
    containing the path to the earth engine asset that will be created, the
    path to the local file that snake make will use to track the asset, and the
    earth engine task (e.g. from ee.batch.Export.*) used to generate the asset.
    Should not start the task.

    Args:
        wait: int, number of seconds to wait between checking if the task has
            completed, set this to be longer for tasks you know will take a
            long time to run, can be set to zero to skip waiting for very short
            running tasks

    Returns:
        func, the input function after being decorated.
    """
    def decorator(func):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ee_input', type=str, required=True)
        parser.add_argument('--ee_output', type=str, required=True)
        parser.add_argument('--local_output', type=str, required=True)

        @functools.wraps(func)
        def inner():
            args, unknownargs = parser.parse_known_args()
            tasks = func(*unknownargs, **vars(args))
            for asset, local_file, task in tasks:
                # ee does not allow overwriting assets that already exist
                try:
                    ee.data.deleteAsset(asset)
                    os.remove(local_file)
                except (ee.EEException, FileNotFoundError):
                    pass  # assume that the file did not already exist
                p = Process(target=_poll, args=(asset, local_file, task, wait))
                p.start()
        return inner
    return decorator


def initialize(rules, config):
    """ Create local copies of all true input files if their gee assets exist.

    It is necessary to run this before running snakemake --summary to ensure
    that the local copies of the "true" inputs (i.e. inputs to rules that are
    not also outputs of other rules) are created.

    Raises an exception if the earth engine asset for a given input does not
    exist.

    This method should be called at the bottom of your snakefile to ensure it
    runs first.

    Args:
        rules: list of snakemake.rules.Rule, accessible within a snakefile as
            `list(workflow.rules)`
        config: dictionary of snakemake configuration parameters, should have
            keys ee_prefix and local_prefix, accessible within a snakefile as
            `config` after setting `configfile: /path/to/config.yaml'

    Returns:
        None
    """
    if 'local_prefix' not in config and 'ee_prefix' not in config:
        # assume there are no earth engine rules
        # therefore there is nothing to do
        return

    all_inputs = set()
    all_outputs = set()
    for rule in rules:
        for file in rule.input:
            all_inputs.add(file)
        for file in rule.output:
            all_outputs.add(file)

    true_inputs = [x for x in all_inputs if x not in all_outputs]

    os.makedirs(config['local_prefix'], exist_ok=True)

    for rule in rules:
        for file in rule.input:
            if not config['local_prefix'] in file:
                continue  # not an ee rule

            try:
                asset = utils.check_update_time(file)
                if asset is not None:
                    utils.write_update_time(asset, file)
            except ee.EEException:  # no remote copy, has a local copy
                if file in true_inputs:
                    raise ValueError(
                        f'EE asset {asset} for {file} does not exist '
                        f'and is not created by a rule.'
                    )
                elif os.path.isfile(file):
                    # remove local copy, forces snakemake to reate remote copy
                    os.remove(file)
            except FileNotFoundError:  # no local copy, possibly a remote copy
                asset = file.replace(
                    config['local_prefix'],
                    config['ee_prefix']
                )
                utils.write_update_time(asset, file)
