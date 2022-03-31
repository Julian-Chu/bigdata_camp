USE julianchu;
with lady as (
SELECT t_user.userid as userid --, count(*) as total 
FROM t_rating 
	JOIN t_user on (t_rating.userid == t_user.userid and t_user.sex == 'F') 
GROUP BY t_user.userid
ORDER BY count(*) DESC
limit 1),
top10 as (
select t_movie.movieid as movieid , t_movie.moviename as moviename, t_rating.rate as rate
from t_rating 
JOIN lady on (lady.userid ==  t_rating.userid) 
JOIN t_movie on (t_movie.movieid == t_rating.movieid)
ORDER BY rate DESC
LIMIT 10)

SELECT * FROM top10;
