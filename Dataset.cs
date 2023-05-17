using System.Data;
using Newtonsoft.Json.Linq;

abstract class Dataset 
{
    protected Dataset(IDbConnection connection) {
        Connection = connection;
    }

    protected IDbConnection Connection { get; }
    public abstract int DatasetId { get; }
    public abstract int RequestId { get; }

    public abstract Task EnsureTablesExist();
    public abstract Task<bool> RecordExistsInDb(int recordId);
    public abstract Task UpdateRecord(JObject record);
    public abstract Task InsertRecord(JObject record);
    
    /// <summary>
    /// Converts a Json element to a string and normalizes apostrophes.
    /// </summary>
    /// <param name="input">Any Json element.</param>
    /// <returns>The string representation of the element, or null if it is null or a blank string.</returns>
    protected static string CleanString(JToken input)
    {
        var s = input?.ToString();
        if (string.IsNullOrWhiteSpace(s))
            return null;
        return s
            .Trim()
            .Replace("'Äô", "'")
            .Replace("’", "'");
    }

    /// <summary>
    /// Converts a Json array to a sequence of strings using <see cref="CleanString"/>.
    /// </summary>
    /// <param name="input">Any Json element.</param>
    /// <returns>
    /// The cleaned, non-null strings in the given array, or an empty sequence if the input is not an array.
    /// </returns>
    protected static IEnumerable<string> JsonArrayToStrings(JToken input) =>
        (input as JArray)?
        .Select(CleanString)
        .Where(value => value is not null) ?? Enumerable.Empty<string>();

    /// <summary>
    /// Converts a Json element to a number and boxes it.
    /// </summary>
    /// <param name="input">Any Json element.</param>
    /// <returns>
    /// If the Json element is a number, returns that number as a boxed int or double, otherwise returns null.
    /// </returns>
    protected static object NumberOrNull(JToken input) {
        if (input?.Type == JTokenType.Integer) {
            return (int?) input;
        } else if (input?.Type == JTokenType.Float) {
            return (double?) input;
        } else if (input?.Type == JTokenType.String) {
            return (int.TryParse(input.ToString(), out var value)) ? value : null;
        } else return null;
    }

    /// <summary>
    /// Uses DateTime.TryParse to attempt to parse the input, returning null on a failure.
    /// </summary>
    protected static DateTime? DateTimeOrNull(JToken input)
    {
        return DateTime.TryParse(input.ToString(), out var result) ? result : null;
    }

    /// <summary>
    /// Returns the single element in a Json array, cleaned with <see cref="CleanString"/>.
    /// </summary>
    /// <param name="input">Any Json element.</param>
    /// <returns>
    /// If the input is an array, returns the cleaned single element of the array.
    /// Returns null if the input is not an array or if the single element is null or blank.
    /// </returns>
    protected static string ArraySingleElement(JToken input)
    {
        if (input is JArray array)
        {
            return CleanString(array.SingleOrDefault());
        }
        else return null;
    }
}