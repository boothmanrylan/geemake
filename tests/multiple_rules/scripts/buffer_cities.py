import ee


def buffer_out(feat):
    return feat.buffer(200000)


def create_tasks(inputs, outputs):
    ee_input_asset = ee.FeatureCollection(inputs[0])
    ee_output_asset = ee_input_asset.map(buffer_out)

    task = ee.batch.Export.table.toAsset(
        collection=ee_output_asset,
        description='test_intermediate_rule_geemake',
        assetId=outputs[0]
    )

    return [(outputs[0], task)]
