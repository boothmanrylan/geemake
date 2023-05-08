import ee


def create_tasks(inputs, outputs):
    ee_input_asset = ee.FeatureCollection(inputs[0])
    ee_output_asset = ee_input_asset.filter(ee.Filter.eq('province', 'on'))

    task = ee.batch.Export.table.toAsset(
        collection=ee_output_asset,
        description='test_first_of_multiple_rules_geemake',
        assetId=outputs[0]
    )

    return [(outputs[0], task)]
