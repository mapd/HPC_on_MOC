import xarray as xr
import gc
from pymapd import connect
import pyarrow as pa
import numpy as np
import pandas as pd
from pyproj import Proj, transform
import mzgeohash
import unicodedata
from datetime import datetime

#define how to process a single short_range_terrain file
def short_range_terrain(file):

	#Open file, create dataframe, cleanup
	ds = xr.open_dataset(file)
	df = ds.to_dataframe()
	df = df.reset_index()
	df = df.drop(['reference_time', 'time', 'ProjectionCoordinateSystem'], axis=1)

	#project columns
	inProj = Proj('+proj=lcc +lat_1=30 +lat_2=60 +lat_0=40.0000076294 +lon_0=-97 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +units=m +no_defs')
	outProj = Proj('+init=epsg:4326')
	x = df['x'].values
	y = df['y'].values
	geo_x, geo_y = transform(inProj,outProj,x,y)

	#add projections to dataframe, clean up
	df['geo_x'] = geo_x
	df['geo_y'] = geo_y
	df = df[ (df['geo_x']<=-93.51) & (df['geo_x']>=-106.65) & (df['geo_y']<=36.5) & (df['geo_y']>=25.84)]
	df['id'] = pd.factorize(((df.geo_x).astype(str))+((df.geo_y).astype(str)))[0]
	df = df.drop(['x', 'y', 'geo_x','geo_y'], axis=1)

	#Define more constants
	phenomenon_time = (ds.attrs['model_output_valid_time']).replace('_'," ")
	phenomenon_time_date =  datetime.strptime(phenomenon_time, '%Y-%m-%d %H:%M:%S')
	phenomenon_time_utc=  phenomenon_time_date.strftime('%Y-%m-%d %H:%M:%S')
	valid_time = (ds.attrs['model_initialization_time']).replace('_'," ")
	valid_time_date = datetime.strptime(valid_time, '%Y-%m-%d %H:%M:%S')
	valid_time_utc = valid_time_date.strftime('%Y-%m-%d %H:%M:%S')

	#Add columns to dataframe
	df['phenomenon_time']= phenomenon_time_utc
	df['valid_time']= valid_time_utc
	df['phenomenon_time'] =  pd.to_datetime(df['phenomenon_time'])
	df['valid_time'] =  pd.to_datetime(df['valid_time'])

	return df

#usage
#df = short_range_terrain("/nwmftp/prod/nwm.20180920/short_range/nwm.t00z.short_range.terrain_rt.f006.conus.nc")

#connect to MapD, load data
#conn=connect(user="mapd", password="HyperInteractive", host="localhost", dbname="mapd")
#conn.load_table("nwm_data_short_term_terrain_test",df,create='infer',method='arrow')
