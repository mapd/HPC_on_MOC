import xarray as xr
from pymapd import connect
import pyarrow as pa
import numpy as np
import pandas as pd
from pyproj import Proj, transform
import mzgeohash
import unicodedata
from datetime import datetime

#define how to process a single analysis_assim_channel file
def analysis_assim_channel(file):

	#Open file, create dataframe, cleanup
	ds = xr.open_dataset(file)
	df = ds.to_dataframe()
	df = df.reset_index()
	df = df.drop(['reference_time', 'time','nudge'], axis=1)

	#Define more constants
	phenomenon_time = (ds.attrs['model_output_valid_time']).replace('_'," ")
	phenomenon_time_date =  datetime.strptime(phenomenon_time, '%Y-%m-%d %H:%M:%S')
	phenomenon_time_utc=  phenomenon_time_date.strftime('%Y-%m-%d %H:%M:%S')

	#Add columns to dataframe
	df['phenomenon_time']=phenomenon_time_utc
	df['valid_time']=phenomenon_time_utc
	df['phenomenon_time'] =  pd.to_datetime(df['phenomenon_time'])
	df['valid_time'] =  pd.to_datetime(df['valid_time'])

	return df


#Usage
#df = analysis_assim_channel("/nwmftp/prod/nwm.20180920/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm00.conus.nc")

#connect to MapD instance, load data
#conn=connect(user="mapd", password="HyperInteractive", host="localhost", dbname="mapd")
#conn.load_table("nwm_channel_test", df, create='infer', method='arrow')
