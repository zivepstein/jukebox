import gspread
import pandas as pd
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import argparse
import random, string


parser = argparse.ArgumentParser()
parser.add_argument("--debug", type=bool, help="run in debug mode without writing to sheets or executing sample.py", default=False)
parser.add_argument("--machine", type=str, help="machine this process is running on", default='p2')
args = parser.parse_args()

# define the scope
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)

def get_current_sheet(creds):
	client = gspread.authorize(creds)
	sheet = client.open('jukebox pipeline')
	sheet_instance = sheet.get_worksheet(0)
	records_data = sheet_instance.get_all_records()
	records_df = pd.DataFrame.from_dict(records_data)
	return records_df

def get_current_run_id(i):
	current_df = get_current_sheet(creds)
	return current_df['run_id'][i]


records_df = get_current_sheet(creds)

hyperparameters = ['model','levels','model','audio_file','prompt_length_in_seconds','sample_length_in_seconds',	'total_sample_length_in_seconds','sr','n_samples','hop_fraction','artist','genre','temp','lyrics', 'mode']
for i,r in records_df.iterrows():
	if r['to_run']==1 and get_current_run_id(i)=='': #now use run_id to find recen
		name =  "{}_{}_{}_prompt{}_dur{}".format(r['audio_file'].split('/')[-1].split('.')[0].replace(" ",""), r['artist'].replace(" ",""), r['genre'].replace(" ",""), r['prompt_length_in_seconds'], r['sample_length_in_seconds'])
		command = ['--name=outputs/{}/{}'.format(r['exp_id'], name)]
		file_path = "outputs/{}/{}".format(r['exp_id'], name)
		for param,value in r.items():
			if param in hyperparameters:
				if isinstance(value, str) and " " in value:
					command.append("--{}='{}'".format(param,value))
				else:
					command.append("--{}={}".format(param,value))
		command_to_run = "python jukebox/sample.py " + " ".join(command)
		run_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
		if not args.debug:
			sheet_instance.update_cell(i+2, 6, run_id) #add run_id
			sheet_instance.update_cell(i+2, 3, datetime.now().strftime("%H:%M:%S, %m/%d/%Y")) # insert start time
			os.system(command_to_run)
			sheet_instance.update_cell(i+2, 4, datetime.now().strftime("%H:%M:%S, %m/%d/%Y")) # insert end time
			sheet_instance.update_cell(i+2, 2, 0) # change 1 -> 0
			sheet_instance.update_cell(i+2, 5, "http://matlaberp2.media.mit.edu:8000/{}/level_0/".format(file_path)) # adding link to drive
			sheet_instance.update_cell(i+2, 7, args.machine) # insert machine this process is running on
		else:
			print('{} launched {} with code  "{}" at {}'.format(i, run_id,command_to_run, datetime.now().strftime("%H:%M:%S %Y")) )
