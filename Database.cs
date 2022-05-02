using System.Data;
using Dapper;
using Newtonsoft.Json.Linq;
using Oracle.ManagedDataAccess.Client;

class Database : IDisposable
{
    /**
     * Mapping a LibInsight dataset to SQL columns:
     * `_id`, `_start_date` and `_entered_by` should always be present, so they are not nullable.
     * Any fillable text input, drop-down list, or radio buttons are mapped as a nullable varchar.
     * Any numeric inputs are mapped as a nullable number.
     * Any multiselect field is represented as a separate table containing the id and a non-nullable varchar.
     * See the `MultiselectFields` list for those definitions.
     */
    const string MainTableCreationSql = @"
        create table ULS_LIBINSIGHT_INST_RECORDS
        (
            RecordId number not null,
            StartDate date not null,
            EnteredBy varchar2(4000) not null,
            EventName varchar2(4000) null,
            FacultySponsorName varchar2(4000) null,
            FacultySponsorEmail varchar2(4000) null,
            Department varchar2(4000) null,
            NumberOfParticipants number null,
            DurationOfEvent number null,
            CoInstructorsOrganisation varchar2(4000) null,
            Notes varchar2(4000) null,
            LocationOfEvent varchar2(4000) null,
            LocationOther varchar2(4000) null,
            EventType varchar2(4000) null,
            ClassNumber number null,
            AdditionalMinutes number null,
            EDI varchar2(4000) null,
            primary key (RecordId)
        );
    ";

    const string UpdateSql = @"
        update ULS_LIBINSIGHT_INST_RECORDS set
            StartDate = :StartDate,
            EnteredBy = :EnteredBy,
            EventName = :EventName,
            FacultySponsorName = :FacultySponsorName,
            FacultySponsorEmail = :FacultySponsorEmail,
            Department = :Department,
            NumberOfParticipants = :NumberOfParticipants,
            DurationOfEvent = :DurationOfEvent,
            CoInstructorsOrganisation = :CoInstructorsOrganisation,
            Notes = :Notes,
            LocationOfEvent = :LocationOfEvent,
            LocationOther = :LocationOther,
            EventType = :EventType,
            ClassNumber = :ClassNumber,
            AdditionalMinutes = :AdditionalMinutes,
            EDI = :EDI
        where RecordId = :RecordId
    ";

    const string InsertSql = @"
        insert into ULS_LIBINSIGHT_INST_RECORDS
        (
            RecordId,
            StartDate,
            EnteredBy,
            EventName,
            FacultySponsorName,
            FacultySponsorEmail,
            Department,
            NumberOfParticipants,
            DurationOfEvent,
            CoInstructorsOrganisation,
            Notes,
            LocationOfEvent,
            LocationOther,
            EventType,
            ClassNumber,
            AdditionalMinutes,
            EDI
        )
        values
        (
            :RecordId,
            :StartDate,
            :EnteredBy,
            :EventName,
            :FacultySponsorName,
            :FacultySponsorEmail,
            :Department,
            :NumberOfParticipants,
            :DurationOfEvent,
            :CoInstructorsOrganisation,
            :Notes,
            :LocationOfEvent,
            :LocationOther,
            :EventType,
            :ClassNumber,
            :AdditionalMinutes,
            :EDI
        )
    ";

    public Database(string connectionString)
    {
        Connection = new OracleConnection(connectionString);
    }

    static List<MultiselectFieldData> MultiselectFields { get; } = new List<MultiselectFieldData>
    {
        new MultiselectFieldData("Topics covered", "ULS_LIBINSIGHT_INST_TOPICS_COVERED", "TopicsCovered", @"
        create table ULS_LIBINSIGHT_INST_TOPICS_COVERED
        (
            RecordId number not null,
            TopicsCovered varchar2(4000) not null
        );"),
        new MultiselectFieldData("Method of delivery", "ULS_LIBINSIGHT_INST_METHOD_OF_DELIVERY", "MethodOfDelivery", @"
        create table ULS_LIBINSIGHT_INST_METHOD_OF_DELIVERY
        (
            RecordId number not null,
            MethodOfDelivery varchar2(4000) not null
        );"),
        new MultiselectFieldData("Audience", "ULS_LIBINSIGHT_INST_AUDIENCE", "Audience", @"
        create table ULS_LIBINSIGHT_INST_AUDIENCE
        (
            RecordId number not null,
            Audience varchar2(4000) not null
        );"),
        new MultiselectFieldData("Skills taught", "ULS_LIBINSIGHT_INST_SKILLS_TAUGHT", "SkillsTaught", @"
        create table ULS_LIBINSIGHT_INST_SKILLS_TAUGHT
        (
            RecordId number not null,
            SkillsTaught varchar2(4000) not null
        );"),
        new MultiselectFieldData("Tools discussed", "ULS_LIBINSIGHT_INST_TOOLS_DISCUSSED", "ToolsDiscussed", @"
        create table ULS_LIBINSIGHT_INST_TOOLS_DISCUSSED
        (
            RecordId number not null,
            ToolsDiscussed varchar2(4000) not null
        );"),
        new MultiselectFieldData("Teaching Consultation Results", "ULS_LIBINSIGHT_INST_TEACHING_CONSULTATION_RESULTS",
            "TeachingConsultationResults", @"
        create table ULS_LIBINSIGHT_INST_TEACHING_CONSULTATION_RESULTS
        (
            RecordId number not null,
            TeachingConsultationResults varchar2(4000) not null
        );"),
    };

    IDbConnection Connection { get; }

    public void Dispose()
    {
        Connection?.Dispose();
    }

    /// <summary>
    /// If any necessary tables do not exist in the database, create them.
    /// </summary>
    public async Task EnsureTablesExist()
    {
        var existingTables = new HashSet<string>(await Connection.QueryAsync<string>(@"
            select table_name
            from user_tables
            where table_name like 'ULS_LIBINSIGHT_INST_%'
        "), StringComparer.OrdinalIgnoreCase);
        if (!existingTables.Contains("ULS_LIBINSIGHT_INST_RECORDS"))
        {
            await Connection.ExecuteAsync(MainTableCreationSql);
        }
        foreach (var field in MultiselectFields.Where(field => !existingTables.Contains(field.TableName)))
        {
            await Connection.ExecuteAsync(field.TableCreationSql);
        }
        
    }

    /// <summary>
    /// Check if a record already exists in the database.
    /// </summary>
    /// <param name="recordId">The record Id. "_id"</param>
    /// <returns>Whether the record is in the database.</returns>
    public async Task<bool> RecordExistsInDb(int recordId)
    {
        var records = await Connection.QueryAsync(
            "select RecordId from ULS_LIBINSIGHT_INST_RECORDS where RecordId = :recordId",
            new { recordId });
        return records.Any();
    }

    /// <summary>
    /// Updates a record in the database with new data from the API.
    /// </summary>
    /// <param name="record">The Json object returned from the API.</param>
    public async Task UpdateRecord(JObject record)
    {
        await Connection.ExecuteAsync(UpdateSql, ToParam(record));
        var recordId = (int)record["_id"];
        foreach (var field in MultiselectFields)
        {
            // Get the values already in the db, and compare with the values in the record to see if there is a difference.
            // Using a HashSet for the set operation and to specify case insensitivity
            var valuesInDb = new HashSet<string>(await Connection.QueryAsync<string>(@$"
                select {field.ColumnName} from {field.TableName}
                where RecordId = :recordId
            ", new { recordId }), StringComparer.CurrentCultureIgnoreCase);
            var valuesInRecord = JsonArrayToStrings(record[field.FieldName]).ToList();
            if (valuesInDb.SetEquals(valuesInRecord)) 
                continue;
            await Connection.ExecuteAsync(@$"
                    delete from {field.TableName}
                    where RecordId = :recordId
                ", new { recordId});
            await Connection.ExecuteAsync(@$"
                    insert into {field.TableName} (RecordId, {field.ColumnName})
                    values (:RecordId, :{field.ColumnName})
                ", valuesInRecord.Select(value => new Dictionary<string, object>
            {
                ["RecordId"] = recordId,
                [field.ColumnName] = value
            }));
        }
    }

    /// <summary>
    /// Insert a new record from the API into the database.
    /// </summary>
    /// <param name="record">The Json object returned from the API.</param>
    public async Task InsertRecord(JObject record)
    {
        await Connection.ExecuteAsync(InsertSql, ToParam(record));
        foreach (var field in MultiselectFields)
        {
            await Connection.ExecuteAsync(@$"
                insert into {field.TableName} (RecordId, {field.ColumnName})
                values (:RecordId, :{field.ColumnName})
            ", JsonArrayToStrings(record[field.FieldName]).Select(value =>
            new Dictionary<string, object>
            {
                ["RecordId"] = (int)record["_id"],
                [field.ColumnName] = value
            }));
        }
    }

    /**
     * The first three fields should always be present and are cast directly.
     * Drop-down lists and radio buttons are returned by the API as either:
     *   - an empty string, if nothing is selected
     *   - an array containing one element
     * `ArraySingleElement` handles both cases, converting empty strings to null or extracting the element from the array.
     * Numeric inputs will also be an empty string if left blank, `NumberOrNull` will convert to null in that case.
     * All other fields will be strings, and should be passed through `CleanString`. 
     */
    static object ToParam(JObject record) => new
    {
        RecordId = (int)record["_id"],
        StartDate = (DateTime?)record["_start_date"],
        EnteredBy = (string)record["_entered_by"],
        EventName = CleanString(record["Event Name (if a class, search for course title and number  here) "]),
        FacultySponsorName = CleanString(record["Faculty/ Sponsor Name"]),
        FacultySponsorEmail = CleanString(record["Faculty/ Sponsor Email"]),
        Department = ArraySingleElement(record["Department"]),
        NumberOfParticipants = NumberOrNull(record["Number of Participants"]),
        DurationOfEvent = NumberOrNull(record["Duration of Event"]),
        CoInstructorsOrganisation = CleanString(record["Co-Instructor(s)/ Organisation"]),
        Notes = CleanString(record["Notes"]),
        LocationOfEvent = ArraySingleElement(record["Location of Event"]),
        LocationOther = CleanString(record["Location - Other"]),
        EventType = ArraySingleElement(record["Event Type"]),
        ClassNumber = NumberOrNull(record["Class Number (5 digits) Available at  Class Search"]),
        AdditionalMinutes = NumberOrNull(record["Additional minutes of prep/follow-up"]),
        EDI = ArraySingleElement(record["Equity, Diversity, Inclusion (EDI)"]),
    };

    /// <summary>
    /// Converts a Json element to a string and normalizes apostrophes.
    /// </summary>
    /// <param name="input">Any Json element.</param>
    /// <returns>The string representation of the element, or null if it is null or a blank string.</returns>
    static string CleanString(JToken input)
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
    static IEnumerable<string> JsonArrayToStrings(JToken input) =>
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
    static object NumberOrNull(JToken input) =>
        input?.Type switch
        {
            JTokenType.Integer => (int?)input,
            JTokenType.Float => (double?)input,
            _ => null
        };

    /// <summary>
    /// Returns the single element in a Json array, cleaned with <see cref="CleanString"/>.
    /// </summary>
    /// <param name="input">Any Json element.</param>
    /// <returns>
    /// If the input is an array, returns the cleaned single element of the array.
    /// Returns null if the input is not an array or if the single element is null or blank.
    /// </returns>
    static string ArraySingleElement(JToken input)
    {
        if (input is JArray array)
        {
            return CleanString(array.SingleOrDefault());
        }
        else return null;
    }

    /// <summary>
    /// Encapsulates information about a multiselect field on the record.
    /// </summary>
    /// <param name="FieldName">The field name in the Json representation returned by the API.</param>
    /// <param name="TableName">The name of the SQL table this field's data is stored in.</param>
    /// <param name="ColumnName">The name of the column containing the data in that table.</param>
    /// <param name="TableCreationSql">The SQL command used to create that table.</param>
    record MultiselectFieldData(string FieldName, string TableName, string ColumnName, string TableCreationSql);
}