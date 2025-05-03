# Spotify API Extract

# Import Libraries

import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):

    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    # Spotify Credentials
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret
))
    
    # Top 50 GLobal Playlist
    playlist_link = "https://open.spotify.com/playlist/6saolYCgadD56Sk9WYcWgx?si=eW0fNdteRjO8blenwKuy"      # TOP 50 GLOBAL
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]

    # Extract Spotify Playlist Tracks
    spotify_data = sp.playlist_tracks(playlist_URI)

    # Stage Spotify Data to S3 Bucket
    client = boto3.client('s3')
    
    filename = "spotify_playlist_top50_global_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket="spotify-bucket-jay",
        Key="staging_data/raw/" + filename,
        Body=json.dumps(spotify_data)
        )
    
    glue = boto3.client('glue')
    gluejobname = "spotify_transformation"

    try:
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status: ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)
        print('Error Starting Glue Job')