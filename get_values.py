import rasterio
import rasterio.features
import rasterio.warp
import geopyspark as gps
import numpy as np
import csv

from datetime import datetime
from pyspark import SparkContext

def get_metadata(line):
    
    try:
        with rasterio.open(line['uri']) as dataset:		
            bounds = dataset.bounds
            height = dataset.height
            width = dataset.width
            crs = dataset.get_crs()
            srs = osr.SpatialReference()
            srs.ImportFromWkt(crs.wkt)
            proj4 = srs.ExportToProj4()
            ws = [w for (ij, w) in dataset.block_windows()]
    except:
            ws = []
            
    def windows(line, ws):
        for w in ws:
            ((row_start, row_stop), (col_start, col_stop)) = w
	    area_of_interest = box(-122.47678756713866, 37.80924146650164, -122.46288299560545, 37.80490143094975)
            new_line['projected_extent'] = gps.TemporalProjectedExtent(extent=extent, instant=instant, proj4=proj4, geometries=area_of_interest)
            left  = bounds.left + (bounds.right - bounds.left)*(float(col_start)/width)
            right = bounds.left + (bounds.right - bounds.left)*(float(col_stop)/ width)
            bottom = bounds.top + (bounds.bottom - bounds.top)*(float(row_stop)/height)
            top = bounds.top + (bounds.bottom - bounds.top)*(float(row_start)/height)
            extent = gps.Extent(left,bottom,right,top)
            instant = datetime.strptime(line['date'], '%Y%j')
                
            new_line = line.copy()
            new_line.pop('date')
            new_line.pop('scene_id')
            new_line['window'] = w
	    yield new_line
    
    return [i for i in windows(line, ws)]

	
def get_data(line):
    
    new_line = line.copy()

    with rasterio.open(line['uri']) as dataset:
        new_line['data'] = dataset.read(1, window=line['window'])
        new_line.pop('window')
        new_line.pop('uri')
    
    return new_line	
	
	
def make_tiles(line):
    projected_extent = line[0]
    bands = sorted(line[1], key=lambda l: l['band'])
    array = np.array([l['data'] for l in bands])
    tile = gps.Tile.from_numpy_array(array, no_data_value=0)
    return (projected_extent, tile)

if __name__ == "__main__":
	
    sc = SparkContext(conf=gps.geopyspark_conf(appName="Landsat").set("spark.ui.enabled",True))
    csv_data = [{'uri': 's3://landsat-pds/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B9.TIF', 'scene_id': 'LC81070352015218LGN00', 'date': '2015218', 'band': '9'},
                {'uri': 's3://landsat-pds/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B9.TIF', 'scene_id': 'LC81070352015218LGN00', 'date': '2015218', 'band': '9'},
                {'uri': 's3://landsat-pds/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B9.TIF', 'scene_id': 'LC81070352015218LGN00', 'date': '2015218', 'band': '9'},
                {'uri': 's3://landsat-pds/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B9.TIF', 'scene_id': 'LC81070352015218LGN00', 'date': '2015218', 'band': '9'}]
    rdd0 = sc.parallelize(csv_data)
    rdd1 = rdd0.flatMap(get_metadata)
    rdd1.first()

    rdd2 = rdd1.map(get_data)
    rdd2.first()

    rdd3 = rdd2.groupBy(lambda line: line['projected_extent'])
    rdd3.first()

    raster_layer = gps.RasterLayer.from_numpy_rdd(gps.LayerType.SPACETIME, num_partitions=840, rdd3)
    tiled_raster_layer = raster_layer.tile_to_layout(layout = gps.GlobalLayout(), target_crs=3857)
    pyramid = tiled_raster_layer.pyramid()
	
    with open('SF_hex.csv', 'r') as SFgrids:
        coord = csv.reader(SFgrids)
    	for layer in pyramid.levels.values(coord):
            gps.write("file:///tmp/values/", "landsat", layer, time_unit=gps.TimeUnit.DAYS)
