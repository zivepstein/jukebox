import gspread
import pandas as pd
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--debug", type=bool, help="run in debug mode without writing to sheets or executing sample.py", default=False)
args = parser.parse_args()

# define the scope
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)

# authorize the clientsheet 
client = gspread.authorize(creds)

sheet = client.open('jukebox pipeline')

# get the first sheet of the Spreadsheet
sheet_instance = sheet.get_worksheet(0)

# get all the records of the data
records_data = sheet_instance.get_all_records()

records_df = pd.DataFrame.from_dict(records_data)
hyperparameters = ['model','levels','model','audio_file','prompt_length_in_seconds',	'sample_length_in_seconds',	'total_sample_length_in_seconds','sr','n_samples','hop_fraction','artist','genre','temp','lyrics', 'mode']
for i,r in records_df.iterrows():
	if r['to_run']==1 and r['start_time']=='':
		name =  "{}_{}_{}_prompt{}_dur{}".format(r['audio_file'].split('/')[-1].split('.')[0].replace(" ",""), r['artist'].replace(" ",""), r['genre'].replace(" ",""), r['prompt_length_in_seconds'], r['sample_length_in_seconds'])
		command = ['--name=outputs/{}/{}'.format(r['exp_id'],name)]
		file_path = command
		for param,value in r.items():
			if param in hyperparameters:
				if isinstance(value, str) and " " in value:
					command.append("--{}='{}'".format(param,value))
				else:
					command.append("--{}={}".format(param,value))
		command_to_run = "python jukebox/sample.py " + " ".join(command)
		# print('{} launched "{}" at {}'.format(i, command_to_run, datetime.now().strftime("%H:%M:%S %Y")) )
		if not args.debug:
			sheet_instance.update_cell(i+2, 3, datetime.now().strftime("%H:%M:%S, %m/%d/%Y")) # insert start time
			os.system(command_to_run)
			sheet_instance.update_cell(i+2, 4, datetime.now().strftime("%H:%M:%S, %m/%d/%Y")) # insert end time
			sheet_instance.update_cell(i+2, 2, 0) # change 1 -> 0
			sheet_instance.update_cell(i+2, 5, '=HYPERLINK("http://matlaberp2.media.mit.edu:8000/{}/level_0/","http://matlaberp2.media.mit.edu:8000/{}/level_0/")'.format(file_path, file_path)) # adding link to drive
		# upload file to drive
