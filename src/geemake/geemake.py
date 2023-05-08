""" Allows Snake Make workflows to work with Earth Engine Assets.
"""
import os
import ee
import geemap
from geemake import utils

geemap.ee_initialize()


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
            if config['local_prefix'] not in file:
                continue  # not an ee input therefore do nothing

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
                os.remove(file)
            except FileNotFoundError:
                asset = file.replace(
                    config['local_prefix'],
                    config['ee_prefix'],
                )
                try:
                    utils.write_update_time(asset, file)
                except ee.EEException:
                    pass
