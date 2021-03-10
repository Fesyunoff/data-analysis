# Analysis of data

## EVENTS

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

## HISTORISITY 

Дана таблица с историчностью (всего около 1 млн. строк).
ID – идентификатор записи
VALUE_DATE - дата появления нового значения параметра
VALUE – значение параметра
```
ID | VALUE_DATE | VALUE
1 | 01/01/2021 | А
1 | 08/01/2021 | Б
2 | 02/01/2021 | В
2 | 03/01/2021 | Ю
2 | 07/01/2021 | С
…
```
• Нужно выбрать все записи и значения их параметра на дату Х (получить состояние таблицы на дату Х). 
Дата появления значения может быть до даты Х, тогда нам нужна ближайшая дата снизу.

## Example
$
<code>
docker-compose up -d postgres 
</code>

$ 
<code>
python3 generator.py e #generate table for 'EVENTS'

python3 generator.py h #generate table for 'HISTORISITY'
</code>

execute SQL code from ./queries
