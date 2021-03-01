# Analysis of data

Есть таблица событий, каждое событие, в числе прочего, содержит следующие колонки:
```
events
  event_timestamp (datetime, not )
  user_id (varchar, not null)
  amount (double, nullable)
  ```
Если событие - платеж, то значение в последней колонке не NULL используя SQL (PosgreSQL), посчитать основные метрики:

– Daily New Users

– Daily Returning Users

– Weekly Active Users)

– Average Revenue Per Paying User

– 7th Day Retention Rate


## Example
$
<code>
docker-compose up postgres 
</code>

$ 
<code>
python3 generator.py
</code>

execute SQL code from queries.sql
