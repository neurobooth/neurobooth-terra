"""
==================================================
Create Entity-Relation diagram from Postgres table
==================================================

This example demonstrates how to create postgres table with neurobooth-terra.
"""
import pygraphviz as pgv

from neurobooth_terra import Table
import psycopg2
import credential_reader as reader

# Initialize connection to database
db_args = reader.read_db_secrets()
connect_str = (f"dbname={db_args['database']} user={db_args['user']}  host={db_args['host']} "
               f"password={db_args['password']} ")

conn = psycopg2.connect(connect_str)

def query(cmd):
    cursor = conn.cursor()
    cursor.execute(cmd)
    conn.commit()

    data = cursor.fetchall()
    cursor.close()
    return data

#### Some useful postgres commands
fk_cmd_base = """
    SELECT
        tc.table_name, 
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 

    FROM
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
    """

query_tables_cmd = """
SELECT *
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND 
    schemaname != 'information_schema';
"""

tables = query(query_tables_cmd)
table_ids = [table[1] for table in tables]

#### Create graph

A = pgv.AGraph(directed=True, repulsiveforce=10.0,
               overlap=False, splines='curved')

# Add nodes
for table_id in table_ids:
    table = Table(table_id, conn)

    rows = []
    rows.append(f'<th><td bgcolor="lightsalmon"><b>{table_id}</b></td></th>')
    for column_name in table.column_names:
        rows.append(f''
        '<tr>'
            f'<td port="{column_name}">{column_name}</td>'
        '</tr>')
    label = "<<table border='0' cellborder='1' cellspacing='0' cellpadding='4'>"
    label += '\n'.join(rows)
    label += "</table>>"
    A.add_node(table_id, shape='plaintext', label=label)

# Add edges
for table_id in table_ids:
    foreign_key_cmd = fk_cmd_base + ("WHERE tc.constraint_type = 'FOREIGN KEY'"
                                    f" AND tc.table_name='{table_id}';")

    fkeys = query(foreign_key_cmd)
    if len(fkeys) > 0:
        for fkey in fkeys:
            fkey_table_id = fkey[2]
            A.add_edge(table_id, fkey_table_id, headport=fkey[1], tailport=fkey[1],
                       arrowtype='normal')

print(A.string())  # print to screen
A.layout(prog='fdp')  # layout with default (neato)
A.draw("er_diagram.pdf")  # draw png
