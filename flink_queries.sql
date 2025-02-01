SELECT * FROM `default`.`cluster_alpha`.`netflix_click_events` LIMIT 10;


SELECT
    CAST(TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm:ss') AS DATE) AS date_day,
    title,
    COUNT(*) AS view_count,
    SUM(CASE WHEN duration > 0 THEN duration ELSE 0 END) AS total_watch_time_seconds,
    AVG(CASE WHEN duration > 0 THEN duration END) AS avg_watch_time_seconds
FROM `default`.`cluster_alpha`.`netflix_click_events`
GROUP BY
    CAST(TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm:ss') AS DATE),
    title;


SELECT
    CAST(TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm:ss') AS DATE) AS date_day,
    movie_id,
    COUNT(*) AS view_count,
    SUM(CASE WHEN duration > 0 THEN duration ELSE 0 END) AS total_watch_time_seconds,
    AVG(CASE WHEN duration > 0 THEN duration END) AS avg_watch_time_seconds
FROM `default`.`cluster_alpha`.`netflix_click_events`
GROUP BY
    CAST(TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm:ss') AS DATE),
    movie_id;


-- fact table
CREATE TABLE daily_movie_engagement (
    date_day DATE,
    movie_id STRING,
    view_count BIGINT,
    total_watch_time_seconds DOUBLE,
    avg_watch_time_seconds DOUBLE,
    PRIMARY KEY (date_day, movie_id) NOT ENFORCED
);


-- populate daily_movie_engagement
INSERT INTO daily_movie_engagement
SELECT
    CAST(TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm:ss') AS DATE) AS date_day,
    movie_id,
    COUNT(*) AS view_count,
    SUM(CASE WHEN duration > 0 THEN duration ELSE 0 END) AS total_watch_time_seconds,
    AVG(CASE WHEN duration > 0 THEN duration END) AS avg_watch_time_seconds
FROM `default`.`cluster_alpha`.`netflix_click_events`
GROUP BY
    CAST(TO_TIMESTAMP(datetime, 'yyyy-MM-dd HH:mm:ss') AS DATE),
    movie_id;


SELECT date_day, movie_id, view_count, total_watch_time_seconds, avg_watch_time_seconds
FROM `daily_movie_engagement`;


CREATE TABLE dim_movies (
    movie_id STRING PRIMARY KEY NOT ENFORCED,
    title STRING,
    genres STRING,
    release_date DATE
);


INSERT INTO dim_movies
SELECT DISTINCT
    movie_id,
    title,
    genres,
    CAST(TO_TIMESTAMP(release_date, 'yyyy-MM-dd') AS DATE) AS release_date
FROM `default`.`cluster_alpha`.`netflix_click_events`;


SELECT movie_id, title, genres, release_date
FROM dim_movies
LIMIT 20;


-- get top 20 movies by total watch time for May 15 of 2019
-- definitely some DQ issues to sort out with more time!
SELECT
    m.title,
    e.total_watch_time_seconds,
    e.view_count,
    e.avg_watch_time_seconds
FROM daily_movie_engagement e
JOIN dim_movies m ON e.movie_id = m.movie_id
WHERE e.date_day = DATE '2019-05-15'
ORDER BY e.total_watch_time_seconds DESC
LIMIT 20;

-- get top 20 movies by total view count for May 15
SELECT
    m.title,
    m.genres,
    e.view_count,
    e.total_watch_time_seconds/3600 as watch_time_hours
FROM daily_movie_engagement e
JOIN dim_movies m ON e.movie_id = m.movie_id
WHERE e.date_day = DATE '2019-05-15'
ORDER BY e.view_count DESC
LIMIT 20;