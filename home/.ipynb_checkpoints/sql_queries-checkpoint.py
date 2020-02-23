import configparser


#Importing from the config file(dwh.cfg)
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES queries
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS Songplay"
user_table_drop = "DROP TABLE IF EXISTS Users"
song_table_drop = "DROP TABLE IF EXISTS Songs"
artist_table_drop = "DROP TABLE IF EXISTS Artist"
time_table_drop = "DROP TABLE IF EXISTS Time"


#=========================================


# staging_events table holds the event data which is fetched from s3://udacity-dend/log_data'
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
    ( 
     artist varchar,
     auth varchar,
     firstName varchar,
     gender varchar,
     itemInSession int,
     lastName varchar,
     length varchar,
     level varchar,
     location varchar,
     method varchar,
     page varchar,
     registration BIGINT,
     sessionId varchar,
     song varchar, 
     status int,
     ts bigint,
     userAgent varchar,
     userId int
     )"""
)
  
    
    
# staging_songs table holds the Song data which is fetched from s3://udacity-dend/song_data
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (  
      artist_id varchar,
      artist_latitude float8, 
      artist_location varchar,
      artist_longitude float8,
      artist_name varchar,
      duration float8,
      num_songs int,
      song_id varchar, 
      title varchar,
      year int
    )"""
)




#=============================================================



#Songplay is the Fact Table here and is connected to other 4 dimension tables
songplay_table_create = (
"""CREATE TABLE IF NOT EXISTS Songplay 
    ( 
     song_id_play int IDENTITY(0,1) PRIMARY KEY, 
     timestamp int not null , 
     userId int not null, 
     level varchar, 
     song_id varchar, 
     artist_id varchar, 
     sessionId varchar, 
     location varchar, 
     userAgent varchar 
    )"""

)
  
# User dimension table consists of data fetched from staging_songs table
user_table_create = (
"""CREATE TABLE IF NOT EXISTS Users 
    ( 
     user_key int not null primary key,
     user_id int not null, 
     firstName varchar, 
     lastName varchar, 
     gender Varchar, 
     level varchar
    ) """ 
)




#Artist dimension table consists of data fetched again from staging_songs table
artist_table_create = (
"""CREATE TABLE IF NOT EXISTS Artist 
   (
    artist_key varchar not null primary key, 
    artist_id varchar not null, 
    artist_name varchar not null , 
    location varchar, 
    latitude float, 
    longitude float
   )"""
)




#Songs dimension table consists of data fetched from staging_events table
song_table_create = (
"""CREATE TABLE IF NOT EXISTS Songs 
    (
     song_key varchar not null primary key,
     song_id varchar not null, 
     title varchar, 
     artist_id varchar, 
     year int, 
     duration float 
    )"""
)




#Time dimension table consists of data fetched from staging_events table
time_table_create = (
"""CREATE TABLE IF NOT EXISTS Time 
   (
    time_key int not null primary key,
    timestamp date not null, 
    hour int not null, 
    day int not null, 
    week int not null, 
    month int not null, 
    year int not null, 
    Weekday int not null
   )"""
)





#===================================================================

# STAGING TABLES
# COPY command is used to copy the data from S3 buckets into both these tables.

staging_events_copy = ("""
            COPY staging_events FROM 's3://udacity-dend/log_data'
            credentials 'aws_iam_role=arn:aws:iam::xxxxxxxxxxxx:role/myRedshiftRole'
            region 'us-west-2'
            format as json {}
            STATUPDATE ON ;
""").format(  config.get('S3','LOG_JSONPATH') )



staging_songs_copy = ("""
            COPY staging_songs(artist_id,artist_latitude,artist_location,artist_longitude,artist_name,duration,num_songs,song_id,title,year) 
            from 's3://udacity-dend/song_data'
            credentials 'aws_iam_role=arn:aws:iam::xxxxxxxxxxxx:role/myRedshiftRole'
            region 'us-west-2'
            JSON 'auto' truncatecolumns
            STATUPDATE ON ;
""").format( )

#==============================================================

# FINAL TABLES

# Data is retieved from song and artist tables and further joined with staging_events before can be inserted into Songplay
songplay_table_insert = ("""
    insert into Songplay( timestamp, userId, level, song_id, artist_id, sessionId, location, userAgent ) 
       Select 
       DISTINCT( TO_CHAR(  TIMESTAMP 'epoch' + SE.ts/1000 * interval '1 second' , 'yyyyMMDD' )::integer ) AS time_key, 
       SE.userId, 
       SE.level,
       SA.song_id, 
       SA.artist_id, 
       SE.sessionId, 
       SE.location, 
       SE.userAgent
       
       from Staging_events SE
       JOIN 
           (  Select s.song_id , a.artist_id , a.artist_name, s.title , s.year, s.duration
                from Songs s
              JOIN Artist a ON (s.artist_id = a.artist_id) ) as SA
              
       ON ( ( SA.title=SE.song) AND (SA.artist_name= SE.artist) AND (SA.duration = SE.length ) )

      WHERE SE.page='NextSong';
"""
)



#Insert queries for the 4 dimension tables.

user_table_insert = ("""
    Insert into Users( user_key, user_id, firstName , lastName , gender, level ) 
    select 
           userId as user_key,  
           userId, 
           firstName, 
           lastName, 
           gender, 
           level
    from staging_events 
    Where userId IS NOT NULL;
""")



song_table_insert = ("""
     Insert into Songs( song_key, song_id, title , artist_id , year, duration ) 
     select 
            song_id as song_key,
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
     from staging_songs;
""")
 


artist_table_insert = ("""
    Insert into Artist( artist_key, artist_id , artist_name , location , latitude , longitude ) 
    select 
           artist_id as artist_key,
           artist_id, 
           artist_name, 
           artist_location as location, 
           artist_latitude as latitude, 
           artist_longitude as longitude
    from staging_songs;
""")



# The time_key is formatted in a suitable fashion to suit the purpose.
# All the columns are extracted from timestamp 'ts' derived from the staging_events table in a peculiar fashion
time_table_insert = ("""
     Insert into Time( time_key, timestamp , hour , day , week , month , year , Weekday ) 
     select 
           DISTINCT( TO_CHAR( ts1, 'yyyyMMDD' )::integer ) AS time_key,
           ts1,
           EXTRACT(hour FROM ts1) AS hour,
           EXTRACT(day FROM ts1) AS day,
           EXTRACT(week FROM ts1) AS week,
           EXTRACT(month FROM ts1) AS month,
           EXTRACT(year FROM ts1) AS year,
           EXTRACT(DOW FROM ts1) As Weekday  
     from  
         ( SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS ts1 FROM staging_events );
""")


#==============================================================


# QUERY LISTS
# A dictionary is maintained to call all respective functions simultaneously

create_table_queries = [ staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create ]


drop_table_queries = [ staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop ]


copy_table_queries = [staging_events_copy, staging_songs_copy]


insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]