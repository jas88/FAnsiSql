-- Test different ways to make SQLite return datetime as proper type
SELECT datetime(event_date, 'start of day') AS method1,
       CAST(datetime(event_date, 'start of day') AS TEXT) AS method2,
       julianday(event_date, 'start of day') AS method3
FROM test_dates LIMIT 1;

-- Test typeof
SELECT typeof(datetime(event_date, 'start of day')) AS type1,
       typeof(CAST(datetime(event_date, 'start of day') AS TEXT)) AS type2,
       typeof(julianday(event_date, 'start of day')) AS type3
FROM test_dates LIMIT 1;
