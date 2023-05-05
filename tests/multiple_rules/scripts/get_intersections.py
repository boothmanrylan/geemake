import ee
import geemap
from geemake import geemake

geemap.ee_initialize()


def get_intersection(feat, col):
    col = col.filter(ee.Filter.neq("name", feat.get("name")))
    intersection = col.map(lambda x: feat.intersection(x.geometry()))
    return ee.Feature(intersection.geometry()).copyProperties(feat)


@geemake.geemake(wait=0)
def create_tasks(ee_input, ee_output, local_output):
    ee_input_asset = ee.FeatureCollection(ee_input)
    ee_output_asset = ee_input_asset.map(
        lambda x: get_intersection(x, ee_input_asset)
    )

    task = ee.batch.Export.table.toAsset(
        collection=ee_output_asset,
        description='test_final_rule_geemake',
        assetId=ee_output
    )

    return [(ee_output, local_output, task)]


if __name__ == "__main__":
    create_tasks()

