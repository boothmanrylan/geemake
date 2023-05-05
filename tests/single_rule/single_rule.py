import ee
import geemap
from geemake import geemake

geemap.ee_initialize()

@geemake.geemake(wait=0)
def create_tasks(ee_input, ee_output, local_output):
    ee_input_asset = ee.FeatureCollection(ee_input)
    yellowknife = ee.FeatureCollection([ee.Feature(
        ee.Geometry.Point(-114.38, 62.45),
        {'name': 'yellowknife', 'province': 'nwt'}
    )])
    ee_output_asset = ee_input_asset.merge(yellowknife)

    task = ee.batch.Export.table.toAsset(
        collection=ee_output_asset,
        description='test_single_rule_geemake',
        assetId=ee_output
    )

    return [(ee_output, local_output, task)]


if __name__ == "__main__":
    create_tasks()

