"""
=============================================
Create Postgres table using Neurobooth-terra.
=============================================

This example demonstrates how to create postgres table with neurobooth-terra.
"""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

###############################################################################
# Let us first import the necessary functions.

from neurobooth_terra import create_table, drop_table

import psycopg2
import pandas as pd

###############################################################################
# Then, we will create a connection using ``psycopg2``.
connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
               "password='neuroboothrocks'")
csv_fname = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
             'register/consent.csv')

conn = psycopg2.connect(connect_str)
cursor = conn.cursor()

###############################################################################
# We will drop tables if they already exist
# this is just for convenience so we can re-run this script
# even when changing some columns
drop_table(conn, cursor, 'subject')
drop_table(conn, cursor, 'contact')
drop_table(conn, cursor, 'consent')

###############################################################################
# Now we define the Table
table_id = 'subject'
table_subject = create_table(conn, cursor, table_id,
                             ['subject_id', 'first_name_birth', 'last_name_birth'],
                             ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)'])

###############################################################################
# and insert some data and retrieve the table as a dataframe
table_subject.insert([('x5dc', 'mainak', 'jas'),
                      ('y5d3', 'anoopum', 'gupta')])
df_subject = table_subject.query(f'SELECT * FROM "{table_id}";')

###############################################################################
# We can create another table and relate it to the other table using
# a foreign key
table_id = 'contact'
table = create_table(conn, cursor, table_id,
                     column_names=['subject_id', 'email'],
                     dtypes=['VARCHAR (255)', 'VARCHAR (255)'],
                     foreign_key=dict(subject_id='subject'))
table.insert([('x5dc',), ('y5d3',)], ['subject_id'])
df_contact = table.query(f'SELECT * FROM "{table_id}";')
print(df_contact)

###############################################################################
# Finally, we can also delete rows in our table
table.delete(condition="subject_id = 'x5dc'")
df_contact = table.query(f'SELECT * FROM "{table_id}";')
print(df_contact)

###############################################################################
# Don't forget to close the connection once done!
cursor.close()
conn.close()
