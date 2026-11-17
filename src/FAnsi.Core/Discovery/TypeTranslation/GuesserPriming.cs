using TypeGuesser;

namespace FAnsi.Discovery.TypeTranslation;

/// <summary>
///     Makes a <see cref="Guesser" /> constructed from a string <see cref="DatabaseTypeRequest" /> start out guessing the
///     width and Unicode of that request.
/// </summary>
/// <remarks>
///     <para>
///         TypeGuesser 2 keeps only the <see cref="DatabaseTypeRequest.CSharpType" /> of the request a <see cref="Guesser" />
///         is constructed with.  <see cref="Guesser.Guess" /> is built from the values seen so far, so until it is shown one
///         the Guesser of an nvarchar(10) column guesses a string of no width that is not Unicode.  A Guesser of a string
///         request is therefore shown one string: as wide as the request and containing a non-ASCII character if the request
///         is Unicode.  Once primed with the string type a Guesser only accepts strings, so this does not change what can be
///         passed to it afterwards.
///     </para>
///     <para>
///         The decimal size of a request is lost in the same way but cannot be restored like this: after seeing a value a
///         Guesser of a numeric type no longer accepts values of the other kind (strings or hard typed values).
///     </para>
/// </remarks>
internal static class GuesserPriming
{
    /// <summary>
    ///     The widest string shown to a <see cref="Guesser" />.  A wider column, such as varchar(max), is primed this wide:
    ///     it is wider than the widest limited string type FAnsi creates on SQL Server and PostgreSQL (8000 characters) and
    ///     on MySQL and Oracle (4000), so the guess is still the unlimited string type there, and no string as wide as the
    ///     column is allocated.
    /// </summary>
    internal const int WidestPrimingValue = 8001;

    /// <summary>
    ///     A character outside ASCII, which tells the Guesser that the strings need a Unicode type
    /// </summary>
    private const char NonAsciiCharacter = 'é';

    /// <summary>
    ///     Shows <paramref name="guesser" /> a string as wide as <paramref name="request" />, containing a non-ASCII
    ///     character if the request is Unicode.  Does nothing unless both the request and the guess are strings.
    /// </summary>
    /// <param name="guesser">A Guesser constructed from <paramref name="request" /></param>
    /// <param name="request">The type the Guesser should start out guessing</param>
    internal static void PrimeWith(Guesser guesser, DatabaseTypeRequest request)
    {
        if (request.CSharpType != typeof(string) || guesser.Guess.CSharpType != typeof(string))
            return;

        var width = request.Width is > 0 ? Math.Min(request.Width.Value, WidestPrimingValue) : 0;

        string value;
        if (request.Unicode)
            //a non-ASCII character counts as 1 + ExtraLengthPerNonAsciiCharacter towards the width of the guess
            value = NonAsciiCharacter +
                    new string('x', Math.Max(0, width - 1 - guesser.ExtraLengthPerNonAsciiCharacter));
        else if (width > 0)
            value = new string('x', width);
        else
            return;

        guesser.AdjustToCompensateForValue(value);
    }
}
