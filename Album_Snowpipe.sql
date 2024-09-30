//Creating a table for the album, artist, and song data.
CREATE OR REPLACE TABLE SPOTIFY_DB_1.PUBLIC.album_table (
    album_id VARCHAR(50),
    name VARCHAR(100),
    release_date DATE,
    total_tracks INT,
    url VARCHAR(255),
    album_type VARCHAR(255)
);

//Creating stage for album data in S3
CREATE OR REPLACE STAGE SPOTIFY_DB_1.PUBLIC.aws_album_stage 
    URL = 's3://etl-project-spotify/transformed_data/album_data/' 
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_file_format;

LIST @SPOTIFY_DB_1.PUBLIC.aws_album_stage;

//Creating the snow pipe and copying the data from the staging area whenever there is new data.
CREATE OR REPLACE pipe  SPOTIFY_DB_1.pipes.album_pipe auto_ingest =TRUE as
COPY INTO SPOTIFY_DB_1.PUBLIC.album_table FROM@SPOTIFY_DB_1.PUBLIC.AWS_ALBUM_STAGE/
   
//Accessing event notifications from S3
DESC pipe SPOTIFY_DB_1.pipes.album_pipe;

//Checking the updated data
select * from SPOTIFY_DB_1.PUBLIC.album_table;

TRUNCATE TABLE SPOTIFY_DB_1.PUBLIC.album_table;
