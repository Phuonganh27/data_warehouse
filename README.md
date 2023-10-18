## Project Description

### Introduction

Sparkify, a music streaming startup, currently stores their data in S3. This data includes JSON logs capturing user activity within the app and JSON metadata for the available songs.

The goal of this project is to build an ETL pipeline that extracts Sparkify data from S3, stages it in Redshift, and performs the necessary transformations to create tables suitable for analytic queries.

## Database Schema

### Raw Data Source

The raw data on user activity is stored in JSON format on S3, with the following file paths as example:

- log_data/2018/11/2018-11-12-events.json
- log_data/2018/11/2018-11-13-events.json

The file `s3://udacity-dend/log_json_path.json` contains the metadata specifying how to read the user activity JSON files.

The master data on song information is also stored in JSON format on S3, with the following file paths as example:

- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

### Staging Tables

The data is copied from S3 into two staging tables: `staging_events_table` and `staging_songs_table`. These tables have columns that match all columns from the raw data source. The table schemas are designed to be flexible to load as much data from the source as possible.

### Final Tables in the Warehouse

The warehouse follows a star schema with the fact table being `songplay_table`. This fact table records an event each time a user starts playing a song. This choice was made because each record represents a unique event or transaction.

## Example Query

With this database schema, the analytics team can answer the following example questions:

- What is the most played song?
- When is the highest usage time of day by hour for songs?
