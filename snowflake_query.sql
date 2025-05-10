CREATE DATABASE spotify_db;

-- Snowflake to Amazon S3 connection

CREATE OR REPLACE storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = "arn:aws:iam::602625286395:role/spotify-snowflake-role"
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-bucket-jay/')
     COMMENT = 'Creating Snowflake connection to Amazon S3'

DESC integration s3_init;

-- Create file format object

CREATE OR REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

-- Stage S3 Spotify transformed data

CREATE OR REPLACE stage spotify_stage
    URL = 's3://spotify-bucket-jay/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

LIST @spotify_stage


-- Create Tables

CREATE OR REPLACE TABLE public.albums (
    album_id STRING,
    name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
);

CREATE OR REPLACE TABLE public.artists (
    artist_id STRING,
    name STRING,
    url STRING
);

CREATE OR REPLACE TABLE public.songs (
    song_id STRING,
    name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);

-- Create Schema for Pipes

CREATE OR REPLACE SCHEMA spotify_pipe;

-- Create Pipes

CREATE OR REPLACE PIPE spotify_pipe.albums_pipe
AUTO_INGEST = TRUE
AS
COPY INTO public.albums
FROM @public.spotify_stage/albums/;

CREATE OR REPLACE PIPE spotify_pipe.artists_pipe
AUTO_INGEST = TRUE
AS
COPY INTO public.artists
FROM @public.spotify_stage/artists/;

CREATE OR REPLACE PIPE spotify_pipe.songs_pipe
AUTO_INGEST = TRUE
AS
COPY INTO public.songs
FROM @public.spotify_stage/songs/;


-- ==========================================

--Join Fact and Dimensions - View For Dashboarding

CREATE OR REPLACE TABLE dim_albums AS
SELECT 
    album_id,
    name AS album_name,
    release_date,
    total_tracks,
    url AS album_url
FROM public.albums;

CREATE OR REPLACE TABLE dim_artists AS
SELECT 
    artist_id,
    name AS artist_name,
    url AS artist_url
FROM public.artists;

CREATE OR REPLACE TABLE fact_songs AS
SELECT 
    s.song_id,
    s.name AS song_name,
    s.duration_ms,
    s.popularity,
    s.song_added,
    s.album_id,
    s.artist_id
FROM public.songs s;


CREATE OR REPLACE VIEW spotify_top_50_songs AS
SELECT 
    fs.song_id,
    fs.song_name,
    da.album_name,
    da.release_date,
    da.total_tracks,
    dar.artist_name,
    fs.duration_ms,
    fs.popularity,
    fs.song_added
FROM public.fact_songs fs
LEFT JOIN public.dim_albums da ON fs.album_id = da.album_id
LEFT JOIN public.dim_artists dar ON fs.artist_id = dar.artist_id
ORDER BY fs.popularity DESC
