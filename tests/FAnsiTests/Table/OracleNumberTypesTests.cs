using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Table;

/// <summary>
///     How GetDataTable reads Oracle's numeric types. ODP.NET reads a NUMBER(p,s) column as the first of short, int,
///     long, float, double and decimal that holds all its values, which .NET 11 converts to decimal exactly.
/// </summary>
internal sealed class OracleNumberTypesTests : DatabaseTests
{
    [Test]
    public void GetDataTable_NumericColumns_HaveTheirTypes()
    {
        var db = GetTestDatabase(DatabaseType.Oracle);
        var tbl = db.CreateTable("NumberTypes",
        [
            new DatabaseColumnRequest("Number52", "NUMBER(5,2)"),
            new DatabaseColumnRequest("Number102", "NUMBER(10,2)"),
            new DatabaseColumnRequest("Number4", "NUMBER(4)"),
            new DatabaseColumnRequest("Float10", "FLOAT(10)"),
            new DatabaseColumnRequest("BinaryFloat", "BINARY_FLOAT"),
            new DatabaseColumnRequest("BinaryDouble", "BINARY_DOUBLE")
        ]);

        try
        {
            using (var con = db.Server.GetConnection())
            {
                con.Open();
                using var cmd = db.Server.GetCommand(
                    $"INSERT INTO {tbl.GetFullyQualifiedName()} VALUES (9.99, 9.99, 10, 9.99, 9.99, 9.99)", con);
                cmd.ExecuteNonQuery();
            }

            // Without enforced types, the columns have the types they are read as
            using var dt = tbl.GetDataTable(enforceTypesAndNullness: false);
            var row = dt.Rows[0];

            Assert.Multiple(() =>
            {
                // ODP.NET reads NUMBER(5,2) as float and NUMBER(10,2) as double: these are read as decimals instead
                Assert.That(dt.Columns["Number52"]?.DataType, Is.EqualTo(typeof(decimal)));
                Assert.That(row["Number52"], Is.EqualTo(9.99m));
                Assert.That(dt.Columns["Number102"]?.DataType, Is.EqualTo(typeof(decimal)));
                Assert.That(row["Number102"], Is.EqualTo(9.99m));

                // The others are read as ODP.NET reads them, which converts NUMBER to double itself
                Assert.That(dt.Columns["Number4"]?.DataType, Is.EqualTo(typeof(short)));
                Assert.That(row["Number4"], Is.EqualTo((short)10));
                Assert.That(dt.Columns["Float10"]?.DataType, Is.EqualTo(typeof(double)));
                Assert.That(Convert.ToDouble(row["Float10"], CultureInfo.InvariantCulture),
                    Is.EqualTo(9.99).Within(1e-12));
                Assert.That(dt.Columns["BinaryFloat"]?.DataType, Is.EqualTo(typeof(float)));
                Assert.That(Convert.ToSingle(row["BinaryFloat"], CultureInfo.InvariantCulture),
                    Is.EqualTo(9.99f).Within(1e-5f));
                Assert.That(dt.Columns["BinaryDouble"]?.DataType, Is.EqualTo(typeof(double)));
                Assert.That(Convert.ToDouble(row["BinaryDouble"], CultureInfo.InvariantCulture),
                    Is.EqualTo(9.99).Within(1e-12));
            });
        }
        finally
        {
            tbl.Drop();
        }
    }
}
