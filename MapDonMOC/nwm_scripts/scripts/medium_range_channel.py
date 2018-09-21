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

def medium_range_channel(file):

	#Open file, create dataframe, cleanup
	ds = xr.open_dataset(file)
	df = ds.to_dataframe()
	df=df.reset_index()
	df=df.drop(['reference_time', 'time','nudge'], axis=1)

	#Define constants
	phenomenon_time = (ds.attrs['model_output_valid_time']).replace('_'," ")
	phenomenon_time_date =  datetime.strptime(phenomenon_time, '%Y-%m-%d %H:%M:%S')
	phenomenon_time_utc=  phenomenon_time_date.strftime('%Y-%m-%d %H:%M:%S')
	valid_time = (ds.attrs['model_initialization_time']).replace('_'," ")
	valid_time_date = datetime.strptime(valid_time, '%Y-%m-%d %H:%M:%S')
	valid_time_utc = valid_time_date.strftime('%Y-%m-%d %H:%M:%S')

	#Add constants to data frame
	df['phenomenon_time']= phenomenon_time_utc
	df['valid_time']= valid_time_utc
	df['phenomenon_time'] =  pd.to_datetime(df['phenomenon_time'])
	df['valid_time'] =  pd.to_datetime(df['valid_time'])

	return df

#usage
#df = medium_range_channel("/nwmftp/prod/nwm.20180920/medium_range/nwm.t00z.medium_range.channel_rt.f003.conus.nc")

#connect to MapD, load data
#conn=connect(user="mapd", password="HyperInteractive", host="localhost", dbname="mapd")
#conn.load_table("nwm_channel_test_mediumrange",df,create='infer',method='arrow')
