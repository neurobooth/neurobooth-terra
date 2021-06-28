import sqlite3
import mysql.connector
import pymysql.cursors

#### Establish Connetion ####

cnx = mysql.connector.connect(user='root', password='n5IaoJkBiAuJvmu0',
# cnx = pymysql.connect(user='root', password='n5IaoJkBiAuJvmu0',
                                  host='35.245.75.54', 
                                  database='neurobooth')

sdfdf
cursor = cnx.cursor()

#### Connetion Established ####

#### Execute query ####

query = """CREATE TABLE `consent` (
	`subject_id` VARCHAR(255) NOT NULL,
	`study_id` VARCHAR(255) NOT NULL,
	`staff_id` VARCHAR(255) NOT NULL,
	`application_id` VARCHAR(255) NOT NULL,
	`site_date` VARCHAR(255) NOT NULL,
	CONSTRAINT `consent_pk` PRIMARY KEY (`subject_id`,`study_id`)
) WITH (
  OIDS=FALSE
);
"""

cursor.execute(query)
cursor.close()
cnx.close()
