import gspread
import pandas as pd
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime


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
params = ['model','levels','model','audio_file','prompt_length_in_seconds',	'sample_length_in_seconds',	'total_sample_length_in_seconds','sr','n_samples','hop_fraction','artist','genre','temp','lyrics']
commands_to_run = []
for i,r in records_df[records_df['to_run']==1].iterrows():
	name =  "{}_{}_{}".format(r['audio_file'].split('/')[-1].split('.')[0].replace(" ",""), r['artist'].replace(" ",""),r['genre'].replace(" ",""))
	print(name)
	command = ['--name={}'.format(name)]
	for param,value in r.items():
		if param in params:
			if isinstance(value, str) and " " in value:
				command.append("--{}='{}'".format(param,value))
			else:
				command.append("--{}={}".format(param,value))
	commands_to_run.append("python jukebox/sample.py " + " ".join(command))

for command_to_run in commands_to_run:
	print('launched "{}" at {}'.format(command_to_run, datetime.now().strftime("%H:%M:%S %Y")) )
	os.system(command_to_run)
