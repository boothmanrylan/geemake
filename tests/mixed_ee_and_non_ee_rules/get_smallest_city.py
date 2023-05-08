import ee


def create_tasks(inputs, outputs):
    ee_input_asset = ee.FeatureCollection(inputs[0])
    ee_output_asset = ee_input_asset.limit(1, 'population')

    task = ee.batch.Export.table.toAsset(
        collection=ee_output_asset,
        description='test_pure_ee_rule',
        assetId=outputs[0]
    )

    return [(outputs[0], task)]
