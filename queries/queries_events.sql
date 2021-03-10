
------------------------------------------------------------
-- Calculate 'Daily New Users' as 'dnu'
------------------------------------------------------------

SELECT 
      DISTINCT(DATE(analytics.events.event_timestamp)) as date
      , COALESCE(new_users, 0) as dnu 
      FROM 
            analytics.events
LEFT JOIN 
        (
        SELECT min_date, COUNT(user_id) AS new_users
        FROM (
                SELECT user_id, MIN(DATE(event_timestamp)) as min_date 
                FROM analytics.events
                GROUP BY user_id
            ) a
        GROUP BY min_date
        ) b ON  DATE(analytics.events.event_timestamp) = b.min_date
ORDER BY date 
;

------------------------------------------------------------
-- Calculate 'Daily Returning Users' as 'dru'
------------------------------------------------------------

SELECT 
      DISTINCT(DATE(analytics.events.event_timestamp)) as date 
      , num_users - COALESCE(new_users, 0) AS dru
FROM
      analytics.events  
LEFT JOIN
        (
        SELECT 
              DATE(event_timestamp) AS date
              , COUNT(DISTINCT user_id) AS num_users 
        FROM 
              analytics.events
        GROUP BY DATE(event_timestamp)
        ) a ON DATE(analytics.events.event_timestamp) = a.date    
LEFT JOIN 
        (
        SELECT 
              min_date
              , COUNT(user_id) AS new_users
        FROM (
                SELECT 
                      user_id
                      , MIN(DATE(event_timestamp)) as min_date 
                FROM analytics.events
                GROUP BY user_id
            ) c
        GROUP BY min_date
        ) b ON  DATE(analytics.events.event_timestamp) = b.min_date
ORDER BY date 
;

------------------------------------------------------------
-- Calculate 'Weekly Active Users' as 'wau'
------------------------------------------------------------

SELECT 
      d.date
      , COUNT(DISTINCT u.user_id) AS wau
FROM ( 
      SELECT 
            DATE(event_timestamp) AS date
      FROM 
            analytics.events
      GROUP BY date
    ) d
JOIN ( 
      SELECT 
            DATE(e2.event_timestamp ) AS date 
            , e2.user_id
      FROM 
            analytics.events e2
      GROUP BY date, e2.user_id
      ) u ON u.date <= d.date AND u.date > ((d.date) - INTERVAL '7 DAY')
GROUP BY d.date
ORDER BY d.date
;


------------------------------------------------------------
-- Calculate 'Average Revenue Per Paying User' as 'arppu'
-- dayly by week (moving average)
------------------------------------------------------------

 SELECT 
      d.date
      , ROUND((sum(amount)/COUNT(DISTINCT user_id))::numeric, 2) as arppu
FROM ( 
      SELECT 
            DATE(event_timestamp) AS date
      FROM 
            analytics.events
      GROUP BY date
    ) d
JOIN ( 
      SELECT 
            DISTINCT DATE(event_timestamp)
            , user_id
            , amount 
      FROM 
            analytics.events
       WHERE 
            amount IS NOT null
      ) u ON u.date <= d.date AND u.date > ((d.date) - INTERVAL '7 DAY')
GROUP BY d.date
ORDER BY d.date
;

------------------------------------------------------------
-- Calculate '7th Day Retention Rate' as 'retention'
------------------------------------------------------------

SELECT 
      *
FROM (
      SELECT 
            date
            , day
            , COUNT(user_id) as retention
      FROM (
          SELECT 
                DISTINCT DATE(event_timestamp) date
                , user_id
                , FIRST_VALUE(DATE(event_timestamp)) 
                OVER (
                      PARTITION BY user_id
                      ORDER BY event_timestamp) as first_appearance 
                , DATE(event_timestamp) - FIRST_VALUE(DATE(event_timestamp)) 
                OVER (
                      PARTITION BY user_id
                      ORDER BY event_timestamp) as day
          FROM 
                analytics.events 
              ) a
          WHERE 
                day  > 0
          AND 
              (DATE_PART('day', date) - DATE_PART('day', first_appearance )) <= 7
      GROUP BY 1, 2) b
ORDER BY 1, 2
;
