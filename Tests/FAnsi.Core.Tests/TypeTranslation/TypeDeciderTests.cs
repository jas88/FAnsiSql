using System;
using System.Globalization;
using NUnit.Framework;
using TypeGuesser;
using TypeGuesser.Deciders;

namespace FAnsiTests.TypeTranslation;

/// <summary>
/// Database-agnostic tests for type deciders (TypeGuesser library)
/// These tests don't require a database connection
/// </summary>
internal sealed class TypeDeciderTests
{
    [Test]
    public void DateTimeTypeDeciderPerformance()
    {
        var d = new DateTimeTypeDecider(new CultureInfo("en-gb"));
        var dt = new DateTime(2019, 5, 22, 8, 59, 36);

        foreach (var f in DateTimeTypeDecider.DateFormatsDM) d.Parse(dt.ToString(f, CultureInfo.InvariantCulture));
        foreach (var f in DateTimeTypeDecider.TimeFormats) d.Parse(dt.ToString(f, CultureInfo.InvariantCulture));

        Assert.That(d.Parse("28/2/1993 5:36:27 AM"), Is.EqualTo(new DateTime(1993, 2, 28, 5, 36, 27)));
    }
}
