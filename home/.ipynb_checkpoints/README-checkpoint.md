In this Project , we build an ETL pipeline that extracts data from S3, stages it in Redshift and transforms it into a set of dimensional tables and a Fact table. We can further create a dimensional cube from the star schema which is generated.

Run the script is following order:-

1. Create_tables.py - It drops and creates the staging tables and the dimension/fact tables on redshift.
2. etl.py - It loads the data from S3 bucket and inserts into these dimension/fact tables.

sql_queries.py is imported by Create_tables.py and etl.py. Its a file which stores all the create and insert queries.

dwh.cfg contains the address to the S3 buckets and other credentials.

Please refer the database schema attached along with - Database_Schema.png.



Sample queries for Drill Down:


1. Query 1 gives all the songs heard(title) by the users(first and last name) in the year 2018.

    select s.title , u.firstName , u.lastName , t.year
    from Users u
    JOIN SongPlay SP ON (SP.userId = u.user_id)
    JOIN Songs s ON (s.song_id = SP.song_id)
    JOIN Time t ON (t.time_key = SP.timestamp)
    where t.year=2018
    group by s.title , u.firstName , u.lastName , t.year ;



2. Query 2 gives all the songs heard(title) by the users(first and last name) in the year 2018 drilled down into months.

    select s.title , u.firstName , u.lastName , t.month
    from Users u
    JOIN SongPlay SP ON (SP.userId = u.user_id)
    JOIN Songs s ON (s.song_id = SP.song_id)
    JOIN Time t ON (t.time_key = SP.timestamp)
    where t.year=2018
    group by s.title , u.firstName , u.lastName , t.month 
    order by t.month;






