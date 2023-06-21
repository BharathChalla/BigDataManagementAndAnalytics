CREATE TABLE rating (userid STRING, itemid STRING, rating FLOAT, ts STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA  INPATH 'ml-100k/u.data' OVERWRITE INTO TABLE rating;

CREATE TABLE item (movieid STRING,  movie STRING, releaseDate STRING,  videoReleaseDate STRING,
              IMDbURL STRING, unknown INT, Action INT, Adventure INT,  Animation INT,
              Childrens INT, Comedy INT, Crime INT, Documentary INT, Drama INT, Fantasy INT,
              FilmNoir INT, Horror INT, Musical INT, Mystery INT, Romance INT, SciFi INT,
              Thriller INT, War INT, Western INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA  INPATH 'ml-100k/u.item' OVERWRITE INTO TABLE item;
LOAD DATA LOCAL INPATH '/home/hadoop/ml-100k/u.item' OVERWRITE INTO TABLE item;


CREATE TABLE movieusers (userid STRING,  age STRING, gender STRING, occupation STRING, zipcode STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

LOAD DATA  INPATH 'ml-100k/u.user' OVERWRITE INTO TABLE movieusers;
LOAD DATA LOCAL INPATH '/home/hadoop/ml-100k/u.user' OVERWRITE INTO TABLE movieusers;

SET hive.execution.engine;
SET hive.execution.engine=tez;
SET hive.execution.engine=spark;
--  https://stackoverflow.com/questions/48982592/hive-how-to-know-which-execution-engine-i-am-currently-using

-- 1. Find the age, gender, and occupation of the user that has written the most reviews.
-- If there is a tie, you can randomly select any.
SELECT u.age, u.gener gender, u.occupation
FROM movieusers as u
WHERE u.userid = (
    SELECT userid
    FROM (
        SELECT r.userid AS userid, COUNT(*) AS ratingsCount
        FROM rating as r
        GROUP BY r.userid
        ORDER BY ratingsCount
    ) LIMIT 1);

-- 2. Create a list of occupations and the number of reviews written by them sorted by the count of reviews in descending order.
-- Example:
-- Student	200000
-- Programmer	18000
-- ....
SELECT m.occupation, COUNT(*) AS ratingsCount
FROM movieusers m JOIN rating r on m.userid = r.userid
GROUP BY m.occupation ORDER BY ratingsCount;

-- 3. Using the u.item table, find the total number of movies of each category
unknown INT, Action INT, Adventure INT,  Animation INT,
              Childrens INT, Comedy INT, Crime INT, Documentary INT, Drama INT, Fantasy INT,
              FilmNoir INT, Horror INT, Musical INT, Mystery INT, Romance INT, SciFi INT,
              Thriller INT, War INT, Western INT
SELECT




-- 4. From the user table, find the number and average age of those with occupation of "scientist"
SELECT COUNT(*), AVG(u.age) FROM movieusers as u WHERE u.occupation = 'scientist' GROUP BY u.occupation;