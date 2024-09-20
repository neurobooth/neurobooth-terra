"""
=========================================================
CRUD operations on Postgres tables using Neurobooth-terra
=========================================================

This example demonstrates how to perform `CRUD`_ operations
on postgres tables with neurobooth-terra.
"""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

# %%
# .. contents:: Table of Contents
#    :local:
#

# %%
# To run this example, it is necessary to set up a local database called
# 'neurobooth' with user 'neuroboother'. Make sure the Postgres server is
# running::
#
#      $ pg_ctl -D /usr/local/var/postgres start
#
# Then create a database 'neurobooth'::
#
#      $ createdb neurobooth
#
# And a user 'neuroboother'::
#
#      $ createuser -P -s -e neuroboother
#
# Now, let us open python or ipython and import the necessary functions.

from neurobooth_terra import list_tables, create_table, drop_table, Table

import psycopg2
import pandas as pd
import scripts.credential_reader as reader
# %%
# Then, we will create a connection using ``psycopg2``.
db_args = reader.read_db_secrets()
connect_str = (f"dbname={db_args['database']} user={db_args['user']}  host={db_args['host']} "
               f"password={db_args['password']} ")

conn = psycopg2.connect(connect_str)

# %%
# We will drop tables if they already exist
# this is just for convenience so we can re-run this script
# even when changing some columns
drop_table('subject', conn)
drop_table('contact', conn)
drop_table('consent', conn)

# %%
# Create
# ------
# Now we define the Table
table_id = 'subject'
column_names = ['subject_id', 'first_name_birth', 'last_name_birth']
dtypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)']
table_subject = create_table(table_id, conn,
                             column_names=column_names,
                             dtypes=dtypes)

# %%
# and insert some data and retrieve the table as a dataframe
table_subject.insert_rows([('x5dc', 'mainak', 'jas'),
                           ('y5d3', 'anoopum', 'gupta')],
                           cols=column_names)

# %%
# We can create another table and relate it to the other table using
# a foreign key
table_id = 'contact'
column_names = ['subject_id', 'email']
dtypes = ['VARCHAR (255)', 'VARCHAR (255)']
table_contact = create_table(table_id, conn,
                             column_names=column_names,
                             dtypes=dtypes,
                             foreign_key=dict(subject_id='subject'))
table_contact.insert_rows([('x5dc',), ('y5d3',)], cols=['subject_id'])

# %% 
# Read
# ----
# We can get a list of tables in the database by doing
list_tables(conn)

# %%
# From the list of these table, we can select one to inspect by
# creating a :class:`~neurobooth_terra.Table` object.
table_id = 'subject'
table_subject = Table(table_id, conn)
print(table_subject)

# %%
# With this Table object, we can create a query that
# returns a dataframe
df_subject = table_subject.query()
print(df_subject)

# %%
# Update
# ------
# We can make changes such as adding a new column
table_subject.add_column('dob', 'VARCHAR (255)')
print(table_subject.query())

# %%
# To update a row in the table we can do
table_subject.update_row('y5d3',
                         cols=['first_name_birth', 'last_name_birth'],
                         vals=('anupum', 'gupta'))
print(table_subject.query())

# %%
# Delete
# ------
# We can also delete rows in our table
table_subject.delete_row(where="subject_id = 'yd53'")
print(table_subject.query())

# %%
# Or drop columns
table_contact.drop_column('email')
print(table_contact.query())

# %%
# To delete an entire table, we can do
drop_table('subject', conn)
list_tables(conn)
# %%
# Don't forget to close the connection once done!
table_subject.close()
table_contact.close()
conn.close()

# %%
# .. _CRUD: https://en.wikipedia.org/wiki/Create,_read,_update_and_delete
