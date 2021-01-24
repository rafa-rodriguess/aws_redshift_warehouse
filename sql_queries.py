import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= "CREATE TABLE IF NOT EXISTS staging_events (artist varchar, auth varchar, firstname varchar, gender varchar, itemInSession int, lastname varchar, length float8, level varchar, location varchar, method varchar, page varchar, registration float8, sessionId varchar, song varchar, status int, ts bigint, userAgent varchar, userId int)"

staging_songs_table_create ='CREATE TABLE IF NOT EXISTS staging_songs (num_songs int, artist_id varchar, artist_latitude real, artist_longitude real, artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration float8, year int)'

songplay_table_create ="CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint IDENTITY(0,1) PRIMARY KEY, start_time varchar NOT NULL, user_id bigint NOT NULL distkey sortkey, level varchar, song_id varchar , artist_id varchar , session_id varchar NOT NULL, location varchar, user_agent varchar, FOREIGN KEY(user_id) REFERENCES users(user_id), FOREIGN KEY(song_id) REFERENCES songs(song_id), FOREIGN KEY(artist_id) REFERENCES artists(artist_id), FOREIGN KEY(start_time) REFERENCES time(start_time)) diststyle key"

user_table_create = "CREATE TABLE IF NOT EXISTS users (user_id bigint PRIMARY KEY sortkey,  first_name varchar, last_name varchar, gender varchar, level varchar) diststyle all"

song_table_create = "CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY sortkey, title varchar, artist_id varchar, year int, duration float8) diststyle all"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY sortkey, name varchar, location varchar, latitude real, longitude real) diststyle all"

time_table_create = "CREATE TABLE IF NOT EXISTS time (start_time varchar PRIMARY KEY sortkey, hour int, day int, week int, month int, year int, weekday int) diststyle all"

# STAGING TABLES

staging_events_copy = ("copy staging_events from {} credentials 'aws_iam_role={}' region {} format as json {};").format(config.get("S3","LOG_DATA") ,config.get("IAM_ROLE","ARN") ,config.get("S3","REGION"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("copy staging_songs from {} credentials 'aws_iam_role={}' region {} json 'auto';").format(config.get("S3","SONG_DATA") ,config.get("IAM_ROLE","ARN") ,config.get("S3","REGION"))

# FINAL TABLES

songplay_table_insert = "INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT timestamp 'epoch' + A.ts / 1000 * interval '1 second', A.userId, A.level, B.song_id, B.artist_id, A.sessionId, A.location, A.userAgent FROM staging_events as A LEFT JOIN songs as B on (A.song = B.title) WHERE page = 'NextSong'"

user_table_insert = "INSERT INTO users SELECT ES.userId, ES.firstName, ES.lastName, ES.gender, ES.level FROM staging_events AS ES join (SELECT max(ts) AS ts, userId FROM staging_events WHERE page = 'NextSong' GROUP BY userId) AS ES2 ON ES.userId = ES2.userId AND ES.ts = ES2.ts"

song_table_insert = "INSERT INTO songs SELECT DISTINCT song_id, title, artist_id, year, duration from staging_songs"

artist_table_insert = "INSERT INTO artists SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs"

time_table_insert = "INSERT INTO time SELECT t.start_time, extract(hour FROM t.start_time), extract(day FROM t.start_time), extract(week FROM t.start_time), extract(month FROM t.start_time), extract(year FROM t.start_time), extract(weekday FROM t.start_time) FROM (SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time FROM staging_events WHERE page = 'NextSong') AS t"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

