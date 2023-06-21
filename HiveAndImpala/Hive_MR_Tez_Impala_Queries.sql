-- wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
-- unzip ml-100k.zip
-- hdfs dfs -put ml-100k /user/hive

-- Ref: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/HiveJDBCDriver.html
-- beeline
-- !connect jdbc:hive2://localhost:10000/default hdfs hdfs

-- Ref: https://blog.ruanbekker.com/blog/2019/04/28/queries-failing-via-beeline-due-to-anonymous-user/
-- Need to put into hdfs before running
-- hdfs dfs -put ml-100k /user/hive
-- beeline -n hadoop -u jdbc:hive2://localhost:10000/default 

-- Enable an SSH Connection Proxy Server
-- https://www.devopszones.com/2020/08/creating-dynamic-ssh-tunnel-using.html

CREATE TABLE rating (userid STRING, itemid STRING, rating FLOAT, ts STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA INPATH '/user/hive/ml-100k/u.data' OVERWRITE INTO TABLE rating;
LOAD DATA LOCAL INPATH '/home/hadoop/ml-100k/u.data' OVERWRITE INTO TABLE rating;


CREATE TABLE item (movieid STRING,  movie STRING, releaseDate STRING,  videoReleaseDate STRING,
              IMDbURL STRING, unknown INT, Action INT, Adventure INT,  Animation INT,
              Childrens INT, Comedy INT, Crime INT, Documentary INT, Drama INT, Fantasy INT,
              FilmNoir INT, Horror INT, Musical INT, Mystery INT, Romance INT, SciFi INT,
              Thriller INT, War INT, Western INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA INPATH '/user/hive/ml-100k/u.item' OVERWRITE INTO TABLE item;
LOAD DATA INPATH 'ml-100k/u.item' OVERWRITE INTO TABLE item;
LOAD DATA LOCAL INPATH '/home/hadoop/ml-100k/u.item' OVERWRITE INTO TABLE item;


CREATE TABLE movieusers (userid STRING,  age INT, gender STRING, occupation STRING, zipcode STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA INPATH '/user/hive/ml-100k/u.user' OVERWRITE INTO TABLE movieusers;
LOAD DATA INPATH 'ml-100k/u.user' OVERWRITE INTO TABLE movieusers;
LOAD DATA LOCAL INPATH '/home/hadoop/ml-100k/u.user' OVERWRITE INTO TABLE movieusers;

SET hive.execution.engine;
SET hive.execution.engine=mr;
SET hive.execution.engine=tez;
SET hive.execution.engine=spark;
-- Error: Error while processing statement: 'SET hive.execution.engine=spark' FAILED in validation : Invalid value.. expects one of [mr, tez]. (state=42000,code=1)
--  https://stackoverflow.com/questions/48982592/hive-how-to-know-which-execution-engine-i-am-currently-using

-- 1. Find the age, gender, and occupation of the user that has written the most reviews.
-- If there is a tie, you can randomly select any.

-- Below Query Don't Work on Impala
SELECT u.age AS Age, u.gender AS Gender, u.occupation AS Occupation
FROM movieusers AS u
WHERE u.userid IN (
    SELECT rcounts.userid
    FROM (
        SELECT r.userid AS userid, COUNT(*) AS ratingsCount
        FROM rating AS r
        GROUP BY r.userid
        ORDER BY ratingsCount DESC
    ) AS rcounts LIMIT 1);

-- When run the query on Impala without the ORDER BY column then gives different results
SELECT u.age AS Age, u.gender AS Gender, u.occupation AS Occupation, rc.ratingsCount AS RC 
FROM (
      SELECT r.userid AS userid, COUNT(*) AS ratingsCount 
      FROM rating AS r 
      GROUP BY r.userid 
      ORDER BY ratingsCount DESC LIMIT 1) as rc 
JOIN movieusers u ON u.userid = rc.userid;


SELECT u.age AS Age, u.gender AS Gender, u.occupation AS Occupation
FROM (
      SELECT r.userid AS userid, COUNT(*) AS ratingsCount 
      FROM rating AS r 
      GROUP BY r.userid 
      ORDER BY ratingsCount DESC LIMIT 1) as rc 
JOIN movieusers u ON u.userid = rc.userid;
-- 2. Create a list of occupations and the number of reviews written by them sorted by the count of reviews in descending order.
-- Example:
-- Student	200000
-- Programmer	18000
-- ....
SELECT m.occupation AS Occupation, COUNT(*) AS ratingsCount
FROM movieusers AS m JOIN rating AS r ON m.userid = r.userid
GROUP BY m.occupation 
ORDER BY ratingsCount DESC LIMIT 21;

-- 3. Using the u.item table, find the total number of movies of each category
-- Categories
-- unknown INT, Action INT, Adventure INT,  Animation INT,
-- Childrens INT, Comedy INT, Crime INT, Documentary INT, Drama INT, Fantasy INT,
-- FilmNoir INT, Horror INT, Musical INT, Mystery INT, Romance INT, SciFi INT,
-- Thriller INT, War INT, Western INT
			  
SELECT 
SUM(i.Action) AS TotalOfAction, 
SUM(i.Adventure) AS TotalOfAdventure, 
SUM(i.Animation) AS TotalOfAnimation, 
SUM(i.Childrens) AS TotalOfChildrens, 
SUM(i.Comedy) AS TotalOfComedy, 
SUM(i.Crime) AS TotalOfCrime, 
SUM(i.Documentary) AS TotalOfDocumentary, 
SUM(i.Drama) AS TotalOfDrama, 
SUM(i.Fantasy) AS TotalOfFantasy, 
SUM(i.FilmNoir) AS TotalOfFilmNoir, 
SUM(i.Horror) AS TotalOfHorror, 
SUM(i.Musical) AS TotalOfMusical, 
SUM(i.Mystery) AS TotalOfMystery, 
SUM(i.Romance) AS TotalOfRomance, 
SUM(i.SciFi) AS TotalOfSciFi, 
SUM(i.Thriller) AS TotalOfThriller, 
SUM(i.War) AS TotalOfWar, 
SUM(i.Western) AS TotalOfWestern,
SUM(i.Unknown) AS TotalOfUnknown
FROM item as i;

-- Formatting Issues - Smaller Column Names
SELECT 
SUM(i.Action) AS Act, 
SUM(i.Adventure) AS Adv, 
SUM(i.Animation) AS Ani, 
SUM(i.Childrens) AS Chi, 
SUM(i.Comedy) AS Com, 
SUM(i.Crime) AS Cri, 
SUM(i.Documentary) AS Doc, 
SUM(i.Drama) AS Dra, 
SUM(i.Fantasy) AS Fan, 
SUM(i.FilmNoir) AS Fil, 
SUM(i.Horror) AS Hor, 
SUM(i.Musical) AS Mus, 
SUM(i.Mystery) AS Mys, 
SUM(i.Romance) AS Rom, 
SUM(i.SciFi) AS Sci, 
SUM(i.Thriller) AS Thr, 
SUM(i.War) AS War, 
SUM(i.Western) AS Wes,
SUM(i.Unknown) AS Unk
FROM item as i;

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/query' 
SELECT 
SUM(i.Action) AS TotalOfAction, 
SUM(i.Adventure) AS TotalOfAdventure, 
SUM(i.Animation) AS TotalOfAnimation, 
SUM(i.Childrens) AS TotalOfChildrens, 
SUM(i.Comedy) AS TotalOfComedy, 
SUM(i.Crime) AS TotalOfCrime, 
SUM(i.Documentary) AS TotalOfDocumentary, 
SUM(i.Drama) AS TotalOfDrama, 
SUM(i.Fantasy) AS TotalOfFantasy, 
SUM(i.FilmNoir) AS TotalOfFilmNoir, 
SUM(i.Horror) AS TotalOfHorror, 
SUM(i.Musical) AS TotalOfMusical, 
SUM(i.Mystery) AS TotalOfMystery, 
SUM(i.Romance) AS TotalOfRomance, 
SUM(i.SciFi) AS TotalOfSciFi, 
SUM(i.Thriller) AS TotalOfThriller, 
SUM(i.War) AS TotalOfWar, 
SUM(i.Western) AS TotalOfWestern,
SUM(i.Unknown) AS TotalOfUnknown
FROM item as i;

hive -e 'SELECT SUM(i.Action) AS TotalOfAction, SUM(i.Adventure) AS TotalOfAdventure, SUM(i.Animation) AS TotalOfAnimation, SUM(i.Childrens) AS TotalOfChildrens, SUM(i.Comedy) AS TotalOfComedy, SUM(i.Crime) AS TotalOfCrime, SUM(i.Documentary) AS TotalOfDocumentary, SUM(i.Drama) AS TotalOfDrama, SUM(i.Fantasy) AS TotalOfFantasy, SUM(i.FilmNoir) AS TotalOfFilmNoir, SUM(i.Horror) AS TotalOfHorror, SUM(i.Musical) AS TotalOfMusical, SUM(i.Mystery) AS TotalOfMystery, SUM(i.Romance) AS TotalOfRomance, SUM(i.SciFi) AS TotalOfSciFi, SUM(i.Thriller) AS TotalOfThriller, SUM(i.War) AS TotalOfWar, SUM(i.Western) AS TotalOfWestern,SUM(i.Unknown) AS TotalOfUnknown FROM item as i;' > out.txt


-- 4. From the user table, find the number and average age of those with occupation of "scientist"
SELECT COUNT(*) NumOfScientists, AVG(u.age) AverageAge 
FROM movieusers AS u 
WHERE u.occupation = 'scientist';





