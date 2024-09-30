CREATE OR REPLACE TABLE SPOTIFY_DB_1.PUBLIC.song_table (
    song_id VARCHAR(50),
    song_name VARCHAR(255),
    duration_ms NUMBER,
    url VARCHAR(255),
    popularity DATE,
    song_added VARCHAR(255),
    album_id VARCHAR(50),
    artist_id VARCHAR(50)
);

CREATE OR REPLACE STAGE SPOTIFY_DB_1.PUBLIC.aws_songs_stage 
    URL = 's3://etl-project-spotify/transformed_data/songs_data/' 
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_file_format;

LIST @SPOTIFY_DB_1.PUBLIC.aws_songs_stage;

//Creating the snow pipe and copying the data from the staging area whenever there is new data.
CREATE OR REPLACE pipe SPOTIFY_DB_1.pipes.song_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO SPOTIFY_DB_1.PUBLIC.song_table FROM@SPOTIFY_DB_1.PUBLIC.aws_songs_stage/;

//Accessing event notifications from S3
DESC pipe SPOTIFY_DB_1.pipes.song_pipe;

//Checking the updated data
select * from SPOTIFY_DB_1.PUBLIC.song_table;

TRUNCATE TABLE SPOTIFY_DB_1.PUBLIC.song_table;