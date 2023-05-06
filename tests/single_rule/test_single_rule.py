import time
import subprocess
import shutil

import pytest

import ee
import geemap

geemap.ee_initialize()

from geemake import utils

OUTPUT = 'users/boothmanrylan/geemake-tests/output'


# enforce geemake to run from within the test directory
# adapted from https://stackoverflow.com/a/62055409
@pytest.fixture(autouse=True)
def change_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)

    yield

    ee.data.deleteAsset(OUTPUT)
    shutil.rmtree(".local")
    shutil.rmtree(".snakemake")


def test_run():
    subprocess.run(["snakemake", "-c1"], check=True, capture_output=True)

    ee_output_asset = ee.FeatureCollection(OUTPUT)

    yellowknife = ee_output_asset.filter(
        ee.Filter.stringStartsWith("name", "yellowknife")
    )

    assert yellowknife.size().getInfo() == 1


def test_rerun():
    subprocess.run(["snakemake", "-c1"], check=True, capture_output=True)

    with open('.local/output', 'r') as f:
        lines = f.readlines()
        asset = lines[0].strip()
        local_update_time = float(lines[1].strip())
        true_update_time = utils.epoch_time(
            ee.data.getAsset(asset)['updateTime']
        )

    time.sleep(10)

    subprocess.run(["snakemake", "-c1"], check=True, capture_output=True)

    with open('.local/output', 'r') as f:
        lines = f.readlines()
        asset = lines[0].strip()
        new_local_update_time = float(lines[1].strip())
        new_true_update_time = utils.epoch_time(
            ee.data.getAsset(asset)['updateTime']
        )

    assert (
        (new_local_update_time == local_update_time) and
        (new_true_update_time == true_update_time) and
        (local_update_time == true_update_time)
    )
