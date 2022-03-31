USE julianchu;
with lady as (
SELECT t_user.userid as userid --, count(*) as total 
FROM t_rating 
	JOIN t_user on (t_rating.userid == t_user.userid and t_user.sex == 'F') 
GROUP BY t_user.userid
ORDER BY count(*) DESC
limit 1),
top10_sort_id_desc as (
select t_movie.movieid as movieid , t_movie.moviename as moviename, t_rating.rate as rate
from t_rating 
JOIN lady on (lady.userid ==  t_rating.userid) 
JOIN t_movie on (t_movie.movieid == t_rating.movieid)
ORDER BY rate, t_movie.movieid DESC
LIMIT 10)

--SELECT * FROM top10_sort_id_desc;
SELECT top10_sort_id_desc.moviename as moveiname, avg(t_rating.rate) as avgrate
FROM top10_sort_id_desc
JOIN t_rating on (top10_sort_id_desc.movieid == t_rating.movieid)
GROUP BY top10_sort_id_desc.movieid, top10_sort_id_desc.moviename
ORDER BY avgrate DESC;
