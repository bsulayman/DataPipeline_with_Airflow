# Project: Data Pipeline with Airflow for Sparkify


## Purpose
The purpose of this project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Sparkify has noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## Database Schema and DAG
As of today, Sparkify collects all their data and store them in [Amazon S3](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend). There are 2 sets of data: [song data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/song_data) and [log data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/log_data).  The song data contains metadata about a song and artist of a song, e.g. artist name, song title, artist location, etc. Its files are partitioned by the first three letters of each song's track ID and it's stored in JSON format. The song data JSON file looks like the following:

```
    {
        "artist_id":"ARJNIUY12298900C91",
        "artist_latitude":null,
        "artist_location":"",
        "artist_longitude":null,
        "artist_name":"Adelitas Way",
        "duration":213.9424,
        "num_songs":1,
        "song_id":"SOBLFFE12AF72AA5BA",
        "title":"Scream",
        "year":2009
    }
```

Meanwhile, log data consists activity log files generated by music streaming app based on the songs in the dataset, e.g. user first name, user last name, artist name, song title, etc. Its files are partitioned by year and month and stored in JSON format. The log data JSON file looks like the following:

```
    {
        "artist":null,
        "auth":"Logged In",
        "firstName":"Walter",
        "gender":"M",
        "itemInSession":0,
        "lastName":"Frye",
        "length":null,
        "level":"free",
        "location":"San Francisco-Oakland-Hayward, CA",
        "method":"GET",
        "page":"Home",
        "registration":1540919166796.0,
        "sessionId":38,
        "song":null,
        "status":200,
        "ts":1541105830796,
        "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
        "userId":"39"
    }
    {
        ...
    }
```

To optimize the query on song play analysis, we will ETL (Extract, Transform, & Load) these data to fact and dimension tables and store them back in AWS S3 bucket. This will also allow us to do on-the-fly at the time of analysis of the data. Below is how the fact and dimension tables will look like:

***Fact Table***

songplays - records in log data associated with song plays i.e. records with page NextSong.<br>
playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent

***Dimension Tables***
1. users - users in the app.<br>
   userid, first_name, last_name, gender, level
2. songs - songs in music database.<br>
   songid, title, artistid, year, duration
3. artists - artists in music database.<br>
   artistid, name, location, latitude, longitude 
4. time - timestamps of records in songplays broken down into specific units.<br>
   start_time, hour, day, week, month, year, weekday


## Instructions
To get started with the project:
1. The project package contains three major components:<br>

	- The dag template has all the imports and task templates in place, but the task dependencies have not been set
	- The operators folder with operator templates
	- A helper class for the SQL transformations
    
2. DAG configuration <br>
	Currently DAG is set to these guidelines:<br>

	- The DAG does not have dependencies on past runs
	- On failure, the task are retried 3 times
	- Retries happen every 5 minutes
	- Catchup is turned off
	- Do not email on retry
    
    The following graph view shows the flow: <br>
    ![DAG](DAG.png)
    
3. As part of this project, we create 4 new operators:<br>
	- Stage Operator (StageToRedshiftOperator)
    	This stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
	- Fact and Dimension Operators (LoadFactOperator & LoadDimensionOperator)
    	With dimension and fact operators, we can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against.
    - Data Quality Operator (DataQualityOperator)
    	This data quality operator is used to run checks on the data itself. Currently, it only checks if any table contains empty record. If there's, it will throw an error
    	
4. Before running Airflow, please add the following Airflow connections through Airflow UI:<br>
	- aws_credentials : Set the connection type as "Amazon Web Services" and enter your aws login & password
	- redshift        : Set the connection type as "Postgres" and enter all your redshift cluster information like host, schema, login, password, and port number


## Author
Budi Sulayman


## License
This project is licensed under [MIT](https://choosealicense.com/licenses/mit/)