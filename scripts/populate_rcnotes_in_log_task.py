# author: Siddharth Patel <spatel@phmi.partners.org>

# Script for inserting research coordinator notes file
# names and task outcome and result file names into the
# log_task table
# A variation of this script can also be used to insert
# 'Nones' into specific column in a table

import os.path as op

import pandas as pd

import psycopg2

from sshtunnel import SSHTunnelForwarder
from neurobooth_terra import Table

ssh_args = dict(
        ssh_address_or_host='neurodoor.nmr.mgh.harvard.edu',
        ssh_username='sp1022',
        ssh_pkey='~/.ssh/id_rsa',
        remote_bind_address=('192.168.100.1', 5432),
        local_bind_address=('localhost', 6543),
        allow_agent=False
)

db_args = dict(
    database='neurobooth', user='neuroboother', password='neuroboothrocks',
)

data_dir = '/autofs/nas/neurobooth/data/'

with SSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:
        # build table dataframes
        table_log_task = Table('log_task', conn)
        table_task = Table('nb_task', conn)
        
        log_task_df = table_log_task.query()
        log_task_df.reset_index(inplace=True)
        
        task_df = table_task.query()
        task_df.reset_index(inplace=True)
        
        # merging tables to get task spellings that are used for notes filenames
        table_joined_df = pd.merge(log_task_df, task_df, on=['task_id'], how='left')
        # for each row in table
        for ix, table_joined_row in table_joined_df.iterrows():
            log_task_id = table_joined_row.log_task_id
            if table_joined_row.subject_id and table_joined_row.date_times:
                # build notes filename and path as per convention
                session_id = table_joined_row.subject_id+'_'+str(table_joined_row.date_times[0].date())
                notes_fname = session_id+'-'+table_joined_row.stimulus_id+'-notes.txt'
                notes_path = op.join(session_id, notes_fname)
                cols = ['log_task_id', 'task_notes_file']
                vals = [(log_task_id, notes_path)]
                # and if that path exists - inser it into database table
                if op.isfile(op.join(data_dir, notes_path)):
                    table_log_task.insert_rows(vals, cols, on_conflict='update')
                    print(vals)
                
                # adding outcomes and results files
                if 'DSC' in table_joined_row.stimulus_id:
                    results = table_joined_row.subject_id+'_'+str(table_joined_row.date_times[0].strftime('%Y-%m-%d_%Hh-%Mm-%Ss'))+'_DSC_results.csv'
                    outcomes = table_joined_row.subject_id+'_'+str(table_joined_row.date_times[0].strftime('%Y-%m-%d_%Hh-%Mm-%Ss'))+'_DSC_outcomes.csv'
                    field = [op.join(session_id, results), op.join(session_id, outcomes)]
                    cols = ['log_task_id', 'task_output_files']
                    vals = [(log_task_id, field)]
                    if op.isfile(op.join(data_dir, field[0])) and op.isfile(op.join(data_dir, field[1])):
                        table_log_task.insert_rows(vals, cols, on_conflict='update')
                        print(vals)
                elif 'MOT' in table_joined_row.stimulus_id:
                    results = table_joined_row.subject_id+'_'+str(table_joined_row.date_times[0].strftime('%Y-%m-%d_%Hh-%Mm-%Ss'))+'_MOT_results.csv'
                    outcomes = table_joined_row.subject_id+'_'+str(table_joined_row.date_times[0].strftime('%Y-%m-%d_%Hh-%Mm-%Ss'))+'_MOT_outcomes.csv'
                    field = [op.join(session_id, results), op.join(session_id, outcomes)]
                    cols = ['log_task_id', 'task_output_files']
                    vals = [(log_task_id, field)]
                    if op.isfile(op.join(data_dir, field[0])) and op.isfile(op.join(data_dir, field[1])):
                        table_log_task.insert_rows(vals, cols, on_conflict='update')
                        print(vals)
