import ee
import pandas as pd


def create_tasks(inputs, outputs):
    census_data = pd.read_csv(inputs.data)
    ee_asset = ee.FeatureCollection(inputs.asset)

    cities_w_pop = []
    for _, row in census_data.iterrows():
        city = ee_asset.filter(ee.Filter.eq('name', row['name'])).first()
        cities_w_pop.append(city.set('population', row['population']))

    output_asset = ee.FeatureCollection(cities_w_pop)

    task = ee.batch.Export.table.toAsset(
        collection=output_asset,
        description='test_adding_local_data',
        assetId=outputs[0]
    )

    return [(outputs[0], task)]
