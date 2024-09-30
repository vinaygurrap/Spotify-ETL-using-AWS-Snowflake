
CREATE OR REPLACE TABLE SPOTIFY_DB_1.PUBLIC.artist_table (
    artist_id VARCHAR(255),
    artist_name VARCHAR(255),
    external_url VARCHAR(255)
);

//Creating stage for album data in S3
CREATE OR REPLACE STAGE SPOTIFY_DB_1.PUBLIC.aws_artist_stage 
    URL = 's3://etl-project-spotify/transformed_data/artist_data/' 
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_file_format;

LIST @SPOTIFY_DB_1.PUBLIC.aws_artist_stage;

//Creating the snow pipe and copying the data from the staging area whenever there is new data.
CREATE OR REPLACE pipe  SPOTIFY_DB_1.pipes.artist_pipe 
    AUTO_INGEST = TRUE
    AS
    COPY INTO SPOTIFY_DB_1.PUBLIC.artist_table FROM@SPOTIFY_DB_1.PUBLIC.aws_artist_stage/;

//Accessing event notifications from S3
DESC pipe SPOTIFY_DB_1.pipes.artist_pipe;

//Checking the updated data
select * from SPOTIFY_DB_1.PUBLIC.artist_table;


TRUNCATE TABLE SPOTIFY_DB_1.PUBLIC.artist_table;