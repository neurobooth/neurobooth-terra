"""
=============================================
Create Postgres table using Neurobooth-terra.
=============================================

This example demonstrates how to create postgres table with neurobooth-terra.
"""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

###############################################################################
# Let us first import the necessary functions.

from neurobooth_terra import list_tables, create_table, drop_table, Table

import psycopg2
import pandas as pd

###############################################################################
# Then, we will create a connection using ``psycopg2``.
connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
               "password='neuroboothrocks'")

conn = psycopg2.connect(connect_str)

###############################################################################
# We will drop tables if they already exist
# this is just for convenience so we can re-run this script
# even when changing some columns
drop_table('subject', conn)
drop_table('contact', conn)
drop_table('consent', conn)

###############################################################################
# Now we define the Table
table_id = 'subject'
table_subject = create_table(table_id, conn,
                             ['subject_id', 'first_name_birth', 'last_name_birth'],
                             ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)'])

###############################################################################
# and insert some data and retrieve the table as a dataframe
table_subject.insert_rows([('x5dc', 'mainak', 'jas'),
                           ('y5d3', 'anoopum', 'gupta')])
df_subject = table_subject.query(f'SELECT * FROM "{table_id}";')
print(df_subject)

###############################################################################
# If we know a table already exists and we want to make modifications to it,
# we can create a Table object first.
table_subject = Table(table_id, conn)
print(table_subject)

###############################################################################
# Then, we can make changes such as adding a new column
table_subject.add_column('dob', 'VARCHAR (255)')
df_subject = table_subject.query(f'SELECT * FROM "{table_id}";')
print(df_subject)

###############################################################################
# We can create another table and relate it to the other table using
# a foreign key
table_id = 'contact'
table = create_table(table_id, conn,
                     column_names=['subject_id', 'email'],
                     dtypes=['VARCHAR (255)', 'VARCHAR (255)'],
                     foreign_key=dict(subject_id='subject'))
table.insert_rows([('x5dc',), ('y5d3',)], ['subject_id'])
df_contact = table.query(f'SELECT * FROM "{table_id}";')
print(df_contact)

###############################################################################
# We can also delete rows in our table
table.delete_row(condition="subject_id = 'x5dc'")
df_contact = table.query(f'SELECT * FROM "{table_id}";')
print(df_contact)

###############################################################################
# Finally, we can list the tables in the database
list_tables(conn)

###############################################################################
# Don't forget to close the connection once done!
table.close()
conn.close()
