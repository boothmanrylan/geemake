import ee


def create_tasks(inputs, outputs):
    ee_input_asset = ee.FeatureCollection(inputs[0])
    yellowknife = ee.FeatureCollection([ee.Feature(
        ee.Geometry.Point(-114.38, 62.45),
        {'name': 'yellowknife', 'province': 'nwt'}
    )])
    ee_output_asset = ee_input_asset.merge(yellowknife)

    task = ee.batch.Export.table.toAsset(
        collection=ee_output_asset,
        description='test_single_rule_geemake',
        assetId=outputs[0]
    )

    return [(outputs[0], task)]
