SELECT t.sec AS sex, t.moviename as name, t.avgrate as avgrate FROM (
    SELECT t_user.age, t_rating.rate 
    FROM t_rating 
    JOIN t_user on (t_rating.userid == t_user.user_id) 
    JOIN t_movie on (t_rating.movieid == t_movie.movieid) 
    WHERE t_user.sex=='M') t 
GROUP BY t.age 
ORDER BY t.age;
