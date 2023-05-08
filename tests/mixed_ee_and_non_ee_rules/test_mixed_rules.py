import os
import time
import subprocess
import shutil
import pandas as pd

import pytest

import ee
import geemap

geemap.ee_initialize()

from geemake import utils

LOCAL_PREFIX = '.local/'
EE_PREFIX = 'users/boothmanrylan/geemake-tests/'
ASSET_1 = EE_PREFIX + 'canadian-cities-w-pop'
ASSET_2 = EE_PREFIX + 'smallest-city'
LOCAL_OUTPUT = 'census.csv'

# enforce geemake to run from within the test directory
# adapted from https://stackoverflow.com/a/62055409
@pytest.fixture(autouse=True)
def change_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)

    yield

    ee.data.deleteAsset(ASSET_1)
    ee.data.deleteAsset(ASSET_2)
    shutil.rmtree(".local")
    shutil.rmtree(".snakemake")
    os.remove("census.csv")


def read_update_times(file):
    with open(file, 'r') as f:
        lines = f.readlines()
        asset = lines[0].strip()
        local_update_time = float(lines[1].strip())
        true_update_time = utils.get_update_time(asset)
        return {'local': local_update_time, 'remote': true_update_time}


def get_update_times():
    update_times = {}
    for output in [ASSET_1, ASSET_2]:
        update_times[output] = read_update_times(
            output.replace(EE_PREFIX, LOCAL_PREFIX)
        )
    return update_times


def test_run():
    subprocess.run(["snakemake", "-c1"], check=True, capture_output=True)

    asset1 = ee.FeatureCollection(ASSET_1)
    asset2 = ee.FeatureCollection(ASSET_2)

    assert (
        os.path.isfile(LOCAL_OUTPUT) and
        (asset1.size().getInfo() == 13) and
        (asset2.size().getInfo() == 1) and
        (asset2.first().get('name').getInfo() == 'stjohns')
    )


def test_rerun():
    subprocess.run(["snakemake", "-c1"], check=True, capture_output=True)
    times = get_update_times()

    time.sleep(10)

    subprocess.run(["snakemake", "-c1"], check=True, capture_output=True)
    new_times = get_update_times()

    assert(
        (times[ASSET_1]['local'] == new_times[ASSET_1]['local']) and
        (times[ASSET_2]['local'] == new_times[ASSET_2]['local']) and
        (times[ASSET_1]['remote'] == new_times[ASSET_1]['remote']) and
        (times[ASSET_2]['remote'] == new_times[ASSET_2]['remote']) and
        (times[ASSET_1]['remote'] == new_times[ASSET_1]['local']) and
        (times[ASSET_2]['local'] == new_times[ASSET_2]['remote'])
    )


def test_rerun_after_local_modification():
    subprocess.run(["snakemake", "-c1"], check=True, capture_output=True)
    times = get_update_times()

    time.sleep(10)

    new_census_data = pd.DataFrame({
        'name': ['victoria', 'vancouver', 'calgary', 'edmonton',
                 'regina', 'winnipeg', 'toronto', 'hamilton',
                 'ottawa', 'montreal', 'quebec', 'halifax', 'stjohns'],
        'population': [360000, 2420000, 1300000, 1150000, 224000,
                       758000, 5600000, 729000, 1060000, 3670000,
                       730000, 348000, 185000]
    })

    os.remove("census.csv")
    new_census_data.to_csv("census.csv", index=False)

    subprocess.run(["snakemake", "-c1"], check=True, capture_output=True)
    new_times = get_update_times()

    assert(
        (times[ASSET_1]['local'] < new_times[ASSET_1]['local']) and
        (times[ASSET_2]['local'] < new_times[ASSET_2]['local']) and
        (times[ASSET_1]['remote'] < new_times[ASSET_1]['remote']) and
        (times[ASSET_2]['remote'] < new_times[ASSET_2]['remote']) and
        (times[ASSET_1]['remote'] < new_times[ASSET_1]['local']) and
        (times[ASSET_2]['local'] < new_times[ASSET_2]['remote'])
    )
