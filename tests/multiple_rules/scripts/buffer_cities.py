import ee
import geemap
from geemake import geemake

geemap.ee_initialize()

def buffer_out(feat):
    return feat.buffer(200000)

@geemake.geemake(wait=0)
def create_tasks(ee_input, ee_output, local_output):
    ee_input_asset = ee.FeatureCollection(ee_input)
    ee_output_asset = ee_input_asset.map(buffer_out)

    task = ee.batch.Export.table.toAsset(
        collection=ee_output_asset,
        description='test_intermediate_rule_geemake',
        assetId=ee_output
    )

    return [(ee_output, local_output, task)]


if __name__ == "__main__":
    create_tasks()
