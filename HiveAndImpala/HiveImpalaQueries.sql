-- Q1
SELECT u.age AS Age, u.gender AS Gender, u.occupation AS Occupation, rc.ratingsCount AS RC 
FROM (
      SELECT r.userid AS userid, COUNT(*) AS ratingsCount 
      FROM rating AS r 
      GROUP BY r.userid 
      ORDER BY ratingsCount DESC LIMIT 1) as rc 
JOIN movieusers u ON u.userid = rc.userid;

-- Q2
SELECT m.occupation AS Occupation, COUNT(*) AS ratingsCount
FROM movieusers AS m JOIN rating AS r ON m.userid = r.userid
GROUP BY m.occupation 
ORDER BY ratingsCount DESC LIMIT 21;

-- Q3
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

-- Q4
SELECT COUNT(*) NumOfScientists, AVG(u.age) AverageAge 
FROM movieusers AS u 
WHERE u.occupation = 'scientist';
