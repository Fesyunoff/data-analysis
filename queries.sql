
------------------------------------------------------------
-- Calculate 'Daily New Users' as 'dnu'
------------------------------------------------------------

SELECT 
      DISTINCT(DATE(analitic.events.event_timestamp)) as date
      , COALESCE(new_users, 0) as dnu 
      FROM 
            analitic.events
LEFT JOIN 
        (
        SELECT min_date, COUNT(user_id) AS new_users
        FROM (
                SELECT user_id, min(DATE(event_timestamp)) as min_date 
                FROM analitic.events
                GROUP BY user_id
            ) a
        GROUP BY min_date
        ) b ON  DATE(analitic.events.event_timestamp) = b.min_date
ORDER BY date 
;
       
------------------------------------------------------------
-- Calculate 'Daily Returning Users' as 'dru'
------------------------------------------------------------

SELECT 
      DISTINCT(DATE(analitic.events.event_timestamp)) as date 
      , num_users - COALESCE(new_users, 0) AS dru
FROM
      analitic.events  
LEFT JOIN
        (
        SELECT 
              DATE(event_timestamp) AS date
              , COUNT(DISTINCT user_id) AS num_users 
        FROM 
              analitic.events
        GROUP BY DATE(event_timestamp)
        ) a ON DATE(analitic.events.event_timestamp) = a.date    
LEFT JOIN 
        (
        SELECT 
              min_date
              , COUNT(user_id) AS new_users
        FROM (
                SELECT 
                      user_id
                      , min(DATE(event_timestamp)) as min_date 
                FROM analitic.events
                GROUP BY user_id
            ) c
        GROUP BY min_date
        ) b ON  DATE(analitic.events.event_timestamp) = b.min_date
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
            analitic.events
      GROUP BY date
    ) d
JOIN ( 
      SELECT 
            DATE(e2.event_timestamp ) AS date 
            , e2.user_id
      FROM 
            analitic.events e2
      GROUP BY date, e2.user_id
      ) u ON u.date <= d.date AND u.date > ((d.date) - INTERVAL '7 DAY')
GROUP BY d.date
ORDER BY d.date
;

------------------------------------------------------------
-- Calculate 'Average Revenue Per Paying User' as 'arppu'
------------------------------------------------------------

SELECT 
      DISTINCT(a.date)
      , a.sum / b.count as arppu
FROM (
      SELECT 
            DATE(e.event_timestamp)
            , sum(e.amount)
            OVER (PARTITION BY DATE(e.event_timestamp))      
      FROM analitic.events e 
      --WHERE amount > 0
      ) a
JOIN (
      SELECT 
            DATE(e.event_timestamp)
            , count(e.user_id)
            OVER (PARTITION BY DATE(e.event_timestamp))      
      FROM analitic.events e 
      WHERE amount > 0
) b ON 
      a.date = b.date
ORDER BY a.date
;

------------------------------------------------------------
-- Calculate '7th Day Retention Rate' as 'retention'
------------------------------------------------------------

SELECT
       s.date 
       , COUNT(s.user_id) AS retention 
FROM (
      SELECT
           user_id
          , date(event_timestamp) as date
      FROM
           analitic.events) s
JOIN
(
       SELECT
            user_id
            , date(event_timestamp)
       FROM
            analitic.events
) e ON 
      s.date =  ((e.date) + INTERVAL '7 DAY')
      AND s.user_id = e.user_id
GROUP BY s.date
ORDER BY s.date
;
