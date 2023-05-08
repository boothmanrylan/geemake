import importlib.util
import time
import os
from multiprocessing import Process

from snakemake.io import InputFiles, OutputFiles

from geemake import utils

import ee
import geemap

geemap.ee_initialize()

extra = snakemake.params.get("extra", "")
log = snakemake.log_fmt_shell(stdout=True, stderr=True)

ee_prefix = snakemake.config.get("ee_prefix", "")
local_prefix = snakemake.config.get("local_prefix", ".local")
script = snakemake.params.get("script", "")
wait = snakemake.params.get("wait", 10)


def poll(asset, local, task):
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
            print(f'Task to create {asset} ended with status: {status}')
            break


def swap_prefix(x):
    return x.replace(local_prefix, ee_prefix)


inputs = InputFiles(toclone=snakemake.input, custom_map=swap_prefix)
outputs = OutputFiles(toclone=snakemake.output, custom_map=swap_prefix)

# load the create_tasks function from the given file
# adapted from:
# https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
spec = importlib.util.spec_from_file_location("geemake_rule", script)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

for asset, task in module.create_tasks(inputs, outputs):
    local_file = asset.replace(ee_prefix, local_prefix)
    try:
        ee.data.deleteAsset(asset)
        os.remove(local_file)
    except (ee.EEException, FileNotFoundError):
        pass
    p = Process(target=poll, args=(asset, local_file, task))
    p.start()
