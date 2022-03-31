USE julianchu;
SELECT t_user.sex as sex,  t_movie.moviename as name, avg(t_rating.rate) as avgrate, count(*) as total 
FROM t_rating 
JOIN t_user on (t_rating.userid == t_user.userid and t_user.sex == 'M') 
JOIN t_movie on (t_rating.movieid == t_movie.movieid) 
GROUP BY t_movie.movieid, t_user.sex, t_movie.moviename 
HAVING count(*) > 50 
ORDER BY avgrate DESC
limit 10;
