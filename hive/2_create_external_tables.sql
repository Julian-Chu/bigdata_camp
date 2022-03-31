USE julianchu;
CREATE EXTERNAL TABLE IF NOT EXISTS t_user (userid int , sex string, age int , occupation int, zipcode int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
STORED AS TEXTFILE
LOCATION '/data/hive/users/';

CREATE EXTERNAL TABLE IF NOT EXISTS t_movie (movieid int , moviename string, movietype string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
STORED AS TEXTFILE
LOCATION '/data/hive/movies/';


CREATE EXTERNAL TABLE IF NOT EXISTS t_rating (userid int , movieid int, rate int, times int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
STORED AS TEXTFILE
LOCATION '/data/hive/ratings/';
