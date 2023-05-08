import ee


def get_intersection(feat, col):
    col = col.filter(ee.Filter.neq("name", feat.get("name")))
    intersection = col.map(lambda x: feat.intersection(x.geometry()))
    return ee.Feature(intersection.geometry()).copyProperties(feat)


def create_tasks(inputs, outputs):
    ee_input_asset = ee.FeatureCollection(inputs[0])
    ee_output_asset = ee_input_asset.map(
        lambda x: get_intersection(x, ee_input_asset)
    )

    task = ee.batch.Export.table.toAsset(
        collection=ee_output_asset,
        description='test_final_rule_geemake',
        assetId=outputs[0]
    )

    return [(outputs[0], task)]
