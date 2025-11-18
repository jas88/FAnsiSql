using Microsoft.Data.Sqlite;
using System;
using System.Data;

class Program
{
    static void Main()
    {
        var connStr = "Data Source=/Users/jas88/Developer/Github/FAnsiSql/Tests/test.db";
        using var conn = new SqliteConnection(connStr);
        conn.Open();

        using var cmd = new SqliteCommand(@"
            SELECT datetime(event_date, 'start of day') AS day_value,
                   typeof(datetime(event_date, 'start of day')) AS type_name
            FROM test_dates LIMIT 1", conn);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var value = reader.GetValue(0);
            var typeName = reader.GetValue(1);

            Console.WriteLine($"Value: {value}");
            Console.WriteLine($"Value Type: {value.GetType().FullName}");
            Console.WriteLine($"SQLite Type: {typeName}");
            Console.WriteLine($"Can cast to DateTime: {value is DateTime}");

            if (value is string str)
            {
                Console.WriteLine($"String value: {str}");
                Console.WriteLine($"Can parse as DateTime: {DateTime.TryParse(str, out var dt)}");
                if (DateTime.TryParse(str, out dt))
                    Console.WriteLine($"Parsed DateTime: {dt}");
            }
        }

        // Also test with DataAdapter
        using var da = new SqliteDataAdapter(@"
            SELECT datetime(event_date, 'start of day') AS day_value
            FROM test_dates", conn);
        var dt = new DataTable();
        da.Fill(dt);

        Console.WriteLine($"\nDataTable column type: {dt.Columns[0].DataType}");
        Console.WriteLine($"First row value type: {dt.Rows[0][0].GetType().FullName}");
        Console.WriteLine($"First row value: {dt.Rows[0][0]}");
    }
}
