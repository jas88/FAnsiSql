using System.Globalization;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Implementations.MicrosoftSQL;
using NUnit.Framework;
using TypeGuesser;
using TypeGuesser.Deciders;

namespace FAnsiTests.TypeTranslation;

/// <summary>
///     Database-agnostic tests for type guessers and deciders (TypeGuesser library)
///     These tests don't require a database connection
/// </summary>
internal sealed class TypeDeciderTests
{
    private readonly ITypeTranslater _translater = MicrosoftSQLTypeTranslater.Instance;

    [Test]
    public void DateTimeTypeDeciderPerformance()
    {
        var d = new DateTimeTypeDecider(new CultureInfo("en-gb"));
        var dt = new DateTime(2019, 5, 22, 8, 59, 36);

        foreach (var f in DateTimeTypeDecider.DateFormatsDM) d.Parse(dt.ToString(f, CultureInfo.InvariantCulture));
        foreach (var f in DateTimeTypeDecider.TimeFormats) d.Parse(dt.ToString(f, CultureInfo.InvariantCulture));

        Assert.That(d.Parse("28/2/1993 5:36:27 AM"), Is.EqualTo(new DateTime(1993, 2, 28, 5, 36, 27)));
    }

    [Test]
    public void TestGuesser_DateTime_English()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("06/05/2001"); // US date format (MM/dd/yyyy)
        t.AdjustToCompensateForValue(null);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(DateTime)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("datetime2"));
        });
    }

    [Test]
    public void TestGuesser_DateTime_EnglishWithTime()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("06/05/2001 11:10"); // US date format with time
        t.AdjustToCompensateForValue(null);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(DateTime)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("datetime2"));
        });
    }

    [Test]
    public void TestGuesser_DateTime_EnglishWithTimeAndAM()
    {
        var t = new Guesser();
        t.AdjustToCompensateForValue("06/05/2001 11:10AM"); // US date format with AM/PM
        t.AdjustToCompensateForValue(null);

        Assert.Multiple(() =>
        {
            Assert.That(t.Guess.CSharpType, Is.EqualTo(typeof(DateTime)));
            Assert.That(t.GetSqlDBType(_translater), Is.EqualTo("datetime2"));
        });
    }
}
