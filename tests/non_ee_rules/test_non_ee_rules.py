import os
import subprocess
import shutil

import pytest

import pandas as pd
import numpy as np

@pytest.fixture(autouse=True)
def change_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)

    yield

    os.remove("input.csv")
    os.remove("intermediate.csv")
    os.remove("output.csv")
    shutil.rmtree(".snakemake")


def test_run_creates_files():
    subprocess.run(["geemake"], check=True, capture_output=True)

    assert (
        os.path.isfile("input.csv") and
        os.path.isfile("intermediate.csv") and
        os.path.isfile("output.csv")
    )


def test_run_creates_proper_input_csv():
    subprocess.run(["geemake"], check=True, capture_output=True)

    input_df = pd.read_csv("input.csv")
    target_df = pd.DataFrame({'A': np.arange(100)})

    assert (input_df == target_df).all().all()


def test_run_creates_proper_intermediate_csv():
    subprocess.run(["geemake"], check=True, capture_output=True)

    intermediate_df = pd.read_csv("intermediate.csv")
    target_df = pd.DataFrame({'A': np.arange(100)})
    target_df['B'] = target_df['A'] ** 2

    assert (intermediate_df == target_df).all().all()


def test_run_creates_proper_output_csv():
    subprocess.run(["geemake"], check=True, capture_output=True)

    output_df = pd.read_csv("output.csv")
    target_df = pd.DataFrame({'A': np.arange(100)})
    target_df['B'] = target_df['A'] ** 2
    target_df['output'] = target_df['A'] + target_df['B']

    assert (output_df == target_df).all().all()




