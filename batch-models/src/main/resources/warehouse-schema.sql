-- Create the 'warehouse' database
CREATE DATABASE warehouse;

-- Connect to the 'warehouse' database
\c warehouse;

-- Table: user_detail
CREATE TABLE user_detail( 
   user_id VARCHAR(255) PRIMARY KEY NOT NULL,
   mdo_id  VARCHAR(255),
   full_name VARCHAR(255),
   designation VARCHAR(255),
   email VARCHAR(255),
   phone_number VARCHAR(255),
   groups VARCHAR(255),
   tag VARCHAR(255),
   user_registration_date DATE,
   roles VARCHAR(255),
   gender VARCHAR(255),
   category VARCHAR(255),
   external_system VARCHAR(255),
   external_system_id VARCHAR(255),
   data_last_generated_on VARCHAR(255)
);

-- Table: org_hierarchy
CREATE TABLE org_hierarchy(
   mdo_id VARCHAR(255) PRIMARY KEY NOT NULL,
   mdo_name  VARCHAR(255),
   is_cbp_provider VARCHAR(64),
   ministry VARCHAR(255),
   department VARCHAR(255),
   organization VARCHAR(255),
   data_last_generated_on DATE
);

-- Table: user_enrolment
CREATE TABLE user_enrolment(
   user_id VARCHAR(255) NOT NULL,
   batch_id VARCHAR(255) NOT NULL,
   cbp_id  VARCHAR(255)  NOT NULL,
   cbp_progress_percentage FLOAT,
   completed_on DATE,
   certificate_generated VARCHAR(255),
   certificate_generated_on DATE,
   user_rating FLOAT,
   resource_count_consumed INTEGER,
   enrolled_on DATE,
   user_consumption_status VARCHAR(255),
   data_last_generated_on VARCHAR(255),
   PRIMARY KEY (user_id, cbp_id, batch_id)
);

-- Table: cbp
CREATE TABLE cbp(
   cbp_id VARCHAR(255) PRIMARY KEY NOT NULL,
   cbp_provider_id VARCHAR(255),
   cbp_provider_name VARCHAR(255),
   cbp_name VARCHAR(255),
   cbp_type VARCHAR(255),
   batch_id VARCHAR(255),
   batch_name VARCHAR(255),
   batch_start_date DATE,
   batch_end_date DATE,
   cbp_duration VARCHAR(255),
   cbp_rating FLOAT,
   last_published_on DATE,
   cbp_retired_on DATE,
   cbp_status VARCHAR(255),
   resource_count INTEGER,
   total_certificates_issued INTEGER,
   cbp_substatus VARCHAR(255),
   data_last_generated_on VARCHAR(255)
);

-- Table: assessment_detail
CREATE TABLE assessment_detail(
   user_id VARCHAR(255)  NOT NULL,
   cbp_id VARCHAR(255) NOT NULL,
   assessment_id VARCHAR(255) NOT NULL,
   assessment_name VARCHAR(255) ,
   assessment_type VARCHAR(255),
   assessment_duration VARCHAR(255),
   time_spent_by_the_user VARCHAR(255),
   completion_date DATE,
   score_achieved FLOAT,
   overall_score FLOAT,
   cut_off_percentage FLOAT,
   total_question INTEGER,
   number_of_incorrect_responses INTEGER,
   number_of_retakes INTEGER,
   data_last_generated_on VARCHAR(255),
   PRIMARY KEY (assessment_id, cbp_id, user_id)
);

-- Table: bp_enrollments
CREATE TABLE bp_enrollments(
   user_id VARCHAR(255) NOT NULL,
   cbp_id  VARCHAR(255) NOT NULL,
   batch_id VARCHAR(255) NOT NULL,
   batch_location VARCHAR(255),
   component_name VARCHAR(255),
   component_id VARCHAR(255),
   component_type VARCHAR(255),
   component_mode VARCHAR(255),
   component_status VARCHAR(255),
   component_duration VARCHAR(255),
   component_progress_percentage FLOAT,
   component_completed_on DATE,
   last_accessed_on DATE,
   offline_session_date DATE,
   offline_session_start_time VARCHAR(255),
   offline_session_end_time VARCHAR(255),
   offline_attendance_status VARCHAR(255),
   instructors_name VARCHAR(255),
   program_coordinator_name VARCHAR(255),
   data_last_generated_on VARCHAR(255)
);

-- Create PostgreSQL user and grant privileges
CREATE USER postgres WITH PASSWORD 'Password@12345678';

GRANT SELECT, INSERT, UPDATE, DELETE ON user_detail TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON org_hierarchy TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON user_enrolment TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON cbp TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON assessment_detail TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON bp_enrollments TO postgres;