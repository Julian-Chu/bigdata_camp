USE julianchu;
SELECT t.age AS age, avg(t.rate) as avgrate FROM (SELECT t_user.age, t_rating.rate FROM t_rating JOIN t_user on (t_rating.userid == t_user.userid) WHERE t_rating.movieid==2116) t GROUP BY t.age ORDER BY t.age;
