------------------------------------------------------------
-- Create a table with historicity (about 1 million rows in total).
-- record_id - record identifier
-- value_date - date when the new parameter value appeared
--              between 2021-01-01 and 2021-03-01
-- value - parameter value
------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE 
  IF NOT EXISTS 
  analytics.historicity(
    record_id INTEGER NOT NULL,
    value_date DATE NOT NULL,
    value INTEGER NOT NULL,
    CONSTRAINT record_date PRIMARY KEY(record_id, value_date)
 );

------------------------------------------------------------
-- Select all records and their parameter values as of date X 
-- (get the state of the table as of date X).
-- Date of occurrence of the value may be before date X, 
-- then we need the nearest date from below.
-- (X = 2021-03-01 for example)
------------------------------------------------------------

SELECT
  record_id, 
  value_date,
  value 
FROM ( 
    SELECT 
      record_id, 
      value_date,
      FIRST_VALUE(value_date) OVER (
        PARTITION  BY record_id
        ORDER BY value_date DESC 
      ) AS next_date, 
      value 
    FROM 
    analytics.historicity h 
  WHERE 
    value_date <= DATE('2021-03-01')
  ) src
WHERE 
next_date = value_date 
;

------------------------------------------------------------
-- Вариант с использованием оконных функций.
------------------------------------------------------------

SELECT
		DISTINCT record_id,
		FIRST_VALUE(value_date) OVER (
      PARTITION  BY record_id
			ORDER BY value_date DESC
	) as value_date,
		first_value(value) over (
		partition  BY record_id
			ORDER BY value_date DESC ) AS value
	FROM
		analytics.historicity h
	WHERE
		 value_date <= DATE('2021-03-01')
  ORDER BY record_id 
;

