import psycopg2
from neurobooth_terra import execute

# drop all tables
"""
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

GRANT ALL ON SCHEMA public TO neuroboother;
GRANT ALL ON SCHEMA public TO public;
"""

###############################################################################
# First, we will create a connection using ``psycopg2``.
connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
               "password='neuroboothrocks'")

conn = psycopg2.connect(connect_str)
cursor = conn.cursor()

###############################################################################
# Then comes our sql command to create the SQL tables. This comes from
# dbdesigner.
# 6/7/2021, 17:02:05
create_cmd = """
CREATE TABLE "consent" (
	"subject_id" VARCHAR(255) NOT NULL,
	"study_id" VARCHAR(255) NOT NULL,
	"staff_id" VARCHAR(255) NOT NULL,
	"application_id" VARCHAR(255) NOT NULL,
	"site_date" VARCHAR(255) NOT NULL,
	CONSTRAINT "consent_pk" PRIMARY KEY ("subject_id","study_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "study" (
	"study_id" VARCHAR(255) NOT NULL,
	"IRB_protocol_number" integer NOT NULL,
	"study_title" VARCHAR(255) NOT NULL,
	"protocol_version_array" integer NOT NULL,
	"protocol_date_array" DATE NOT NULL,
	"consent_version_array" VARCHAR(255) NOT NULL,
	"consent_date_array" DATE NOT NULL,
	CONSTRAINT "study_pk" PRIMARY KEY ("study_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "register" (
	"subject_id" VARCHAR(255) NOT NULL,
	"study_id" VARCHAR(255) NOT NULL,
	"application_id" VARCHAR(255) NOT NULL,
	"site_date" VARCHAR(255) NOT NULL,
	CONSTRAINT "register_pk" PRIMARY KEY ("subject_id","study_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "contact" (
	"subject_id" VARCHAR(255) NOT NULL,
	"study_id" VARCHAR(255) NOT NULL,
	"application_id" VARCHAR(255) NOT NULL,
	"site_date" VARCHAR(255) NOT NULL,
	"email" VARCHAR(255) NOT NULL,
	"phone" VARCHAR(255) NOT NULL,
	CONSTRAINT "contact_pk" PRIMARY KEY ("subject_id","study_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "clinical" (
	"subject_id" VARCHAR(255) NOT NULL,
	"study_id" VARCHAR(255) NOT NULL,
	"application_id" VARCHAR(255) NOT NULL,
	"site_date" VARCHAR(255) NOT NULL,
	"diagnosis" VARCHAR(255) NOT NULL,
	"relevant_diagnosis" VARCHAR(255) NOT NULL,
	"symptom_age_onset" integer NOT NULL,
	"handedness" VARCHAR(255) NOT NULL,
	"height" FLOAT NOT NULL,
	"weight" FLOAT NOT NULL,
	"medications" TEXT NOT NULL,
	"MRN" integer NOT NULL,
	CONSTRAINT "clinical_pk" PRIMARY KEY ("subject_id","study_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "subject" (
	"subject_id" VARCHAR(255) PRIMARY KEY,
	"first_name_birth" VARCHAR(255) NOT NULL,
	"middle_name_birth" VARCHAR(255) NOT NULL,
	"last_name_birth" VARCHAR(255) NOT NULL,
	"date of birth" DATE NOT NULL,
	"country of birth" VARCHAR(255) NOT NULL,
	"gender at birth" VARCHAR(255) NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "demograph" (
	"subject_id" VARCHAR(255) NOT NULL,
	"study_id" VARCHAR(255) NOT NULL,
	"application_id" VARCHAR(255) NOT NULL,
	"site_date" VARCHAR(255) NOT NULL,
	"date_of_birth" DATE NOT NULL,
	"age_study_start" integer NOT NULL,
	"symptom_age_onset" integer NOT NULL,
	"sex" VARCHAR(255) NOT NULL,
	"race" VARCHAR(255) NOT NULL,
	"ethnicity" VARCHAR(255) NOT NULL,
	"recruit_mechanism" VARCHAR(255) NOT NULL,
	CONSTRAINT "demograph_pk" PRIMARY KEY ("subject_id","study_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "human_obs_log" (
	"subject_id" VARCHAR(255) NOT NULL,
	"study_id" VARCHAR(255) NOT NULL,
	"observer_id" VARCHAR(255) NOT NULL,
	"human_obs_id" VARCHAR(255) NOT NULL,
	"application_id" VARCHAR(255) NOT NULL,
	"site_date" DATE NOT NULL,
	"response_array" VARCHAR(255) NOT NULL,
	CONSTRAINT "human_obs_log_pk" PRIMARY KEY ("subject_id","study_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "observer" (
	"observer_id" VARCHAR(255) NOT NULL,
	"subject_id" VARCHAR(255) NOT NULL,
	"first_name" VARCHAR(255) NOT NULL,
	"last_name" VARCHAR(255) NOT NULL,
	"age" integer NOT NULL,
	"sex" VARCHAR(255) NOT NULL,
	"race" VARCHAR(255) NOT NULL,
	"ethnicity" VARCHAR(255) NOT NULL,
	"relationship" VARCHAR(255) NOT NULL,
	CONSTRAINT "observer_pk" PRIMARY KEY ("observer_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "tech_obs_data" (
	"tech_obs_id" VARCHAR(255) NOT NULL,
	"obs_name" VARCHAR(255) NOT NULL,
	"feature_of_interest" VARCHAR(255) NOT NULL,
	"measurement_type" VARCHAR(255) NOT NULL,
	"instruction_id" VARCHAR(255) NOT NULL,
	"demo_id" VARCHAR(255) NOT NULL,
	"stimulus_id" VARCHAR(255) NOT NULL,
	"device_id_array" VARCHAR(255) NOT NULL,
	"question_array" VARCHAR(255) NOT NULL,
	"response_array" VARCHAR(255) NOT NULL,
	"obs_property_array" VARCHAR(255) NOT NULL,
	"obs_time_period_array" VARCHAR(255) NOT NULL,
	"units_array" VARCHAR(255) NOT NULL,
	"sensor_id_array" VARCHAR(255) NOT NULL,
	"file_id_array" VARCHAR(255) NOT NULL,
	"sensor_param_array" VARCHAR(255) NOT NULL,
	"device-stim_sync_array" VARCHAR(255) NOT NULL,
	"interdevice_sync-matrix" VARCHAR(255) NOT NULL,
	CONSTRAINT "tech_obs_data_pk" PRIMARY KEY ("tech_obs_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "tech_obs_log" (
	"subject_id" VARCHAR(255) NOT NULL,
	"study_id" VARCHAR(255) NOT NULL,
	"tech_obs_id" VARCHAR(255) NOT NULL,
	"staff_id" VARCHAR(255) NOT NULL,
	"application_id" VARCHAR(255) NOT NULL,
	"site_date" DATE NOT NULL,
	"event_array" VARCHAR(255) NOT NULL,
	"date_time_array" VARCHAR(255) NOT NULL,
	CONSTRAINT "tech_obs_log_pk" PRIMARY KEY ("subject_id","study_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "stimulus" (
	"stimulus_id" VARCHAR(255) NOT NULL,
	"stimulus_description" VARCHAR(255) NOT NULL,
	"num_iterations" VARCHAR(255) NOT NULL,
	"duration" FLOAT NOT NULL,
	"stimulus_filetype" VARCHAR(255) NOT NULL,
	"stimulus_file" VARCHAR(255) NOT NULL,
	CONSTRAINT "stimulus_pk" PRIMARY KEY ("stimulus_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "sensor" (
	"sensor_id" VARCHAR(255) NOT NULL,
	"temporal_res" FLOAT NOT NULL,
	"spatial_res" FLOAT NOT NULL,
	CONSTRAINT "sensor_pk" PRIMARY KEY ("sensor_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "instruction" (
	"instruction_id" VARCHAR(255) PRIMARY KEY,
	"instruction_text" TEXT NOT NULL,
	"instruction_filetype" VARCHAR(255) NOT NULL,
	"instruction_file" VARCHAR(255) NOT NULL
) WITH (
  OIDS=FALSE
);



CREATE TABLE "human_obs_data" (
	"human_obs_id" VARCHAR(255) NOT NULL,
	"obs_name" VARCHAR(255) NOT NULL,
	"feature_of_interest" VARCHAR(255) NOT NULL,
	"measurement_type" VARCHAR(255) NOT NULL,
	"question_array" VARCHAR(255) NOT NULL,
	"response_array" VARCHAR(255) NOT NULL,
	"obs_property_array" VARCHAR(255) NOT NULL,
	"obs_time_period_array" VARCHAR(255) NOT NULL,
	"units_array" VARCHAR(255) NOT NULL,
	CONSTRAINT "human_obs_data_pk" PRIMARY KEY ("human_obs_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "device" (
	"device_id" VARCHAR(255) NOT NULL,
	"device_sn" VARCHAR(255) NOT NULL,
	"wearable_bool" BOOLEAN NOT NULL,
	"device_location" VARCHAR(255) NOT NULL,
	"device_name" VARCHAR(255) NOT NULL,
	"device_make" VARCHAR(255) NOT NULL,
	"device_model" VARCHAR(255) NOT NULL,
	"device_firmware" VARCHAR(255) NOT NULL,
	"sensor_id_array" BOOLEAN NOT NULL,
	CONSTRAINT "device_pk" PRIMARY KEY ("device_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "collection" (
	"collection_name" VARCHAR(255) NOT NULL,
	"tech_obs_array" VARCHAR(255),
	"human_obs_array" VARCHAR(255),
	CONSTRAINT "collection_pk" PRIMARY KEY ("collection_name")
) WITH (
  OIDS=FALSE
);



ALTER TABLE "consent" ADD CONSTRAINT "consent_fk0" FOREIGN KEY ("subject_id") REFERENCES "subject"("subject_id");
ALTER TABLE "consent" ADD CONSTRAINT "consent_fk1" FOREIGN KEY ("study_id") REFERENCES "study"("study_id");


ALTER TABLE "register" ADD CONSTRAINT "register_fk0" FOREIGN KEY ("subject_id") REFERENCES "subject"("subject_id");
ALTER TABLE "register" ADD CONSTRAINT "register_fk1" FOREIGN KEY ("study_id") REFERENCES "subject"("subject_id");

ALTER TABLE "contact" ADD CONSTRAINT "contact_fk0" FOREIGN KEY ("subject_id") REFERENCES "subject"("subject_id");
ALTER TABLE "contact" ADD CONSTRAINT "contact_fk1" FOREIGN KEY ("study_id") REFERENCES "study"("study_id");

ALTER TABLE "clinical" ADD CONSTRAINT "clinical_fk0" FOREIGN KEY ("subject_id") REFERENCES "subject"("subject_id");
ALTER TABLE "clinical" ADD CONSTRAINT "clinical_fk1" FOREIGN KEY ("study_id") REFERENCES "study"("study_id");


ALTER TABLE "demograph" ADD CONSTRAINT "demograph_fk0" FOREIGN KEY ("subject_id") REFERENCES "subject"("subject_id");
ALTER TABLE "demograph" ADD CONSTRAINT "demograph_fk1" FOREIGN KEY ("study_id") REFERENCES "study"("study_id");

ALTER TABLE "human_obs_log" ADD CONSTRAINT "human_obs_log_fk0" FOREIGN KEY ("subject_id") REFERENCES "subject"("subject_id");
ALTER TABLE "human_obs_log" ADD CONSTRAINT "human_obs_log_fk1" FOREIGN KEY ("study_id") REFERENCES "study"("study_id");
ALTER TABLE "human_obs_log" ADD CONSTRAINT "human_obs_log_fk2" FOREIGN KEY ("observer_id") REFERENCES "observer"("observer_id");
ALTER TABLE "human_obs_log" ADD CONSTRAINT "human_obs_log_fk3" FOREIGN KEY ("human_obs_id") REFERENCES "human_obs_data"("human_obs_id");


ALTER TABLE "tech_obs_data" ADD CONSTRAINT "tech_obs_data_fk0" FOREIGN KEY ("instruction_id") REFERENCES "instruction"("instruction_id");
ALTER TABLE "tech_obs_data" ADD CONSTRAINT "tech_obs_data_fk1" FOREIGN KEY ("stimulus_id") REFERENCES "stimulus"("stimulus_id");

ALTER TABLE "tech_obs_log" ADD CONSTRAINT "tech_obs_log_fk0" FOREIGN KEY ("subject_id") REFERENCES "subject"("subject_id");
ALTER TABLE "tech_obs_log" ADD CONSTRAINT "tech_obs_log_fk1" FOREIGN KEY ("study_id") REFERENCES "study"("study_id");
ALTER TABLE "tech_obs_log" ADD CONSTRAINT "tech_obs_log_fk2" FOREIGN KEY ("tech_obs_id") REFERENCES "tech_obs_data"("tech_obs_id");






ALTER TABLE "collection" ADD CONSTRAINT "collection_fk0" FOREIGN KEY ("tech_obs_array") REFERENCES "tech_obs_data"("tech_obs_id");
ALTER TABLE "collection" ADD CONSTRAINT "collection_fk1" FOREIGN KEY ("human_obs_array") REFERENCES "human_obs_data"("human_obs_id");


"""
###############################################################################
# Now we create the tables.
execute(conn, cursor, create_cmd)

###############################################################################
# Don't forget to close the connection once done!
cursor.close()
conn.close()
