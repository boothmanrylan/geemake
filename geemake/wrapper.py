import os
from snakemake.shell import shell

extra = snakemake.params.get("extra", "")
log = snakemake.log_fmt_shell(stdout=True, stderr=True)

ee_prefix = snakemake.config.get("ee_prefix", "")
local_prefix = snakemake.config.get("local_prefix", ".geemake_local")
script = snakemake.params.get("script", "")

# create local_prefix directory if it does not yet exist
os.makedirs(local_prefix, exist_ok=True)

def swap_prefix(x):
    return x.replace(local_prefix, ee_prefix)

# get the earth engine asset file names
inputs = [swap_prefix(x) for x in snakemake.input]
outputs = [swap_prefix(x) for x in snakemake.output]

shell(
    "python {script} {extra} "
    "--ee_input {inputs} "
    "--ee_output {outputs} "
    "--local_output {snakemake.output} "
    "{log}"
)
