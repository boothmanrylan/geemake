import os
import time
import subprocess
import shutil

import pytest

import ee
import geemap

geemap.ee_initialize()

from geemake import geemake
from geemake import utils

EE_PREFIX = 'users/boothmanrylan/geemake-tests/'
LOCAL_PREFIX = '.local/'
SUBSET_OUTPUT = EE_PREFIX + 'canadian-cities-subset'
BUFFER_OUTPUT = EE_PREFIX + 'buffered-cities'
INTERSECTION_OUTPUT = EE_PREFIX + 'intersecting-city-regions'


# enforce geemake to run from within the test directory
# adapted from https://stackoverflow.com/a/62055409
@pytest.fixture(autouse=True)
def change_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)

    yield

    ee.data.deleteAsset(SUBSET_OUTPUT)
    ee.data.deleteAsset(BUFFER_OUTPUT)
    ee.data.deleteAsset(INTERSECTION_OUTPUT)
    shutil.rmtree(".local")
    shutil.rmtree(".snakemake")


def read_update_times(file):
    with open(file, 'r') as f:
        lines = f.readlines()
        asset = lines[0].strip()
        local_update_time = float(lines[1].strip())
        true_update_time = utils.epoch_time(
            ee.data.getAsset(asset)['updateTime']
        )
        return {'local': local_update_time, 'remote': true_update_time}


def get_update_times():
    update_times = {}
    for output in [SUBSET_OUTPUT, BUFFER_OUTPUT, INTERSECTION_OUTPUT]:
        update_times[output] = read_update_times(
            output.replace(EE_PREFIX, LOCAL_PREFIX)
        )
    return update_times


def test_run():
    subprocess.run(["geemake"], check=True, capture_output=True)

    asset1 = ee.FeatureCollection(SUBSET_OUTPUT)
    asset2 = ee.FeatureCollection(BUFFER_OUTPUT)
    asset3 = ee.FeatureCollection(INTERSECTION_OUTPUT)

    assert (
        (asset1.size().getInfo() == 3) and
        (asset2.size().getInfo() == 3) and
        (asset3.size().getInfo() == 3)
    )


def test_rerun():
    subprocess.run(["geemake"], check=True, capture_output=True)
    update_times = get_update_times()

    time.sleep(10)

    subprocess.run(["geemake"], check=True, capture_output=True)
    new_update_times = get_update_times()

    local_times_match_remote_times = sum([
        new_update_times[x]['remote'] == new_update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_local = sum([
        new_update_times[x]['local'] == update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_remote = sum([
        new_update_times[x]['remote'] == update_times[x]['remote']
        for x in update_times.keys()
    ])

    assert (
        (local_times_match_remote_times == 3) and
        (new_times_match_old_times_local == 3) and
        (new_times_match_old_times_remote == 3)
    )


def test_rerun_after_local_modification():
    subprocess.run(["geemake"], check=True, capture_output=True)
    update_times = get_update_times()

    time.sleep(10)

    os.remove(SUBSET_OUTPUT.replace(EE_PREFIX, LOCAL_PREFIX))
    subprocess.run(["geemake", "subset_cities"], check=True, capture_output=True)
    subprocess.run(["geemake"], check=True, capture_output=True)
    new_update_times = get_update_times()

    local_times_match_remote_times = sum([
        new_update_times[x]['remote'] == new_update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_local = sum([
        new_update_times[x]['local'] == update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_remote = sum([
        new_update_times[x]['remote'] == update_times[x]['remote']
        for x in update_times.keys()
    ])

    assert (
        (local_times_match_remote_times == 3) and
        (new_times_match_old_times_local == 3) and
        (new_times_match_old_times_remote == 3)
    )


def test_rerun_after_remote_modification1():
    subprocess.run(["geemake"], check=True, capture_output=True)
    update_times = get_update_times()

    time.sleep(10)

    ee.data.deleteAsset(SUBSET_OUTPUT)
    subprocess.run(["geemake", "subset_cities"], check=True, capture_output=True)
    subprocess.run(["geemake"], check=True, capture_output=True)
    new_update_times = get_update_times()

    local_times_match_remote_times = sum([
        new_update_times[x]['remote'] == new_update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_local = sum([
        new_update_times[x]['local'] > update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_remote = sum([
        new_update_times[x]['remote'] > update_times[x]['remote']
        for x in update_times.keys()
    ])

    assert (
        (local_times_match_remote_times == 3) and
        (new_times_match_old_times_local == 3) and
        (new_times_match_old_times_remote == 3)
    )


def test_rerun_after_remote_modification2():
    subprocess.run(["geemake"], check=True, capture_output=True)
    update_times = get_update_times()

    time.sleep(10)

    ee.data.deleteAsset(INTERSECTION_OUTPUT)
    subprocess.run(["geemake", "intersection"], check=True, capture_output=True)
    subprocess.run(["geemake"], check=True, capture_output=True)
    new_update_times = get_update_times()

    local_times_match_remote_times = sum([
        new_update_times[x]['remote'] == new_update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_local = sum([
        new_update_times[x]['local'] == update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_remote = sum([
        new_update_times[x]['remote'] == update_times[x]['remote']
        for x in update_times.keys()
    ])

    assert (
        (local_times_match_remote_times == 3) and
        (new_times_match_old_times_local == 2) and
        (new_times_match_old_times_remote == 2) and
        (new_update_times[INTERSECTION_OUTPUT]['remote'] >
            update_times[INTERSECTION_OUTPUT]['remote']) and
        (new_update_times[INTERSECTION_OUTPUT]['local'] >
            update_times[INTERSECTION_OUTPUT]['local'])
    )


def test_forced_rerun():
    subprocess.run(["geemake"], check=True, capture_output=True)
    update_times = get_update_times()

    time.sleep(10)

    subprocess.run(["geemake", "--forceall"], check=True, capture_output=True)
    new_update_times = get_update_times()

    local_times_match_remote_times = sum([
        new_update_times[x]['remote'] == new_update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_gt_old_times_local = sum([
        new_update_times[x]['local'] > update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_gt_old_times_remote = sum([
        new_update_times[x]['remote'] > update_times[x]['remote']
        for x in update_times.keys()
    ])

    assert (
        (local_times_match_remote_times == 3) and
        (new_times_gt_old_times_local == 3) and
        (new_times_gt_old_times_remote == 3)
    )


def test_forced_partial_rerun():
    subprocess.run(["geemake"], check=True, capture_output=True)
    update_times = get_update_times()

    time.sleep(10)

    subprocess.run(
        ["geemake", "-f", "intersection"],
        check=True,
        capture_output=True
    )
    new_update_times = get_update_times()

    local_times_match_remote_times = sum([
        new_update_times[x]['remote'] == new_update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_local = sum([
        new_update_times[x]['local'] == update_times[x]['local']
        for x in new_update_times.keys()
    ])

    new_times_match_old_times_remote = sum([
        new_update_times[x]['remote'] == update_times[x]['remote']
        for x in update_times.keys()
    ])

    assert (
        (local_times_match_remote_times == 3) and
        (new_times_match_old_times_local == 2) and
        (new_times_match_old_times_remote == 2) and
        (new_update_times[INTERSECTION_OUTPUT]['remote'] >
            update_times[INTERSECTION_OUTPUT]['remote']) and
        (new_update_times[INTERSECTION_OUTPUT]['local'] >
            update_times[INTERSECTION_OUTPUT]['local'])
    )
