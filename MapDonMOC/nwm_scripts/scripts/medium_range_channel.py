import xarray as xr
import pandas as pd
from pyproj import Proj, transform
import mzgeohash
import unicodedata
from datetime import datetime
import glob

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

#Could switch to if __main__():
if True:

    #Load previously processed files list
    with open('/nwmftp/processed/processedfiles.txt') as f:
        processedfiles = f.read().splitlines()

    #Get all analysis_assim files for tm00, channel and 0|6|12|18
    files = [x for x in glob.glob("/nwmftp/prod/**/medium_range/*channel*.conus.nc", recursive=True)
	if any(w in x for w in ('t00', 't06', 't12', 't18'))
	and any(w in x for w in ('f024', 'f030', 'f036', 'f042', 'f048', 'f054', 'f060', 'f066',
	'f072', 'f078', 'f084', 'f090', 'f096', 'f102', 'f108', 'f114', 'f120', 'f126', 'f132',
	'f138', 'f144', 'f150', 'f156', 'f162', 'f168', 'f174', 'f180', 'f186', 'f192', 'f198',
	'f204', 'f210', 'f216', 'f222', 'f228', 'f234', 'f240'))
	]

    #Get files not already processed
    needprocessing = [x for x in files if x not in processedfiles]

    #Process files
    for fn in needprocessing:
        try:
            tmpdf = medium_range_channel(fn)
            tmpdf.to_csv("/nwmftp/processed/files/{0}.csv.gz".format(fn.split('/')[-1]), index=False, compression='gzip')
            with open('/nwmftp/processed/processedfiles.txt', 'a') as f:
                f.write(fn + "\n")
        except:
            print("error!") #TODO: should make a warning somewhere
