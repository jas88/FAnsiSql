-- Test what SQLite datetime() function returns
CREATE TABLE test_dates (id INTEGER, event_date TEXT);
INSERT INTO test_dates VALUES (1, '2001-01-01 00:00:00');
INSERT INTO test_dates VALUES (2, '2001-01-02 00:00:00');

-- Test datetime() function
SELECT datetime(event_date, 'start of day') AS day_value FROM test_dates;

-- Test what type it actually returns
SELECT typeof(datetime(event_date, 'start of day')) AS type_name FROM test_dates LIMIT 1;
