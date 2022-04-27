using Newtonsoft.Json.Linq;
using Dapper;
using System.Data;
using Oracle.ManagedDataAccess.Client;

class Database : IDisposable
{
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

    const string TableCreationSql = @"
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
        create table ULS_LIBINSIGHT_INST_TOPICS_COVERED
        (
            RecordId number not null,
            TopicsCovered varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INST_METHOD_OF_DELIVERY
        (
            RecordId number not null,
            MethodOfDelivery varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INST_AUDIENCE
        (
            RecordId number not null,
            Audience varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INST_SKILLS_TAUGHT
        (
            RecordId number not null,
            SkillsTaught varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INST_TOOLS_DISCUSSED
        (
            RecordId number not null,
            ToolsDiscussed varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INST_TEACHING_CONSULTATION_RESULTS
        (
            RecordId number not null,
            TeachingConsultationResults varchar2(4000) not null
        );
    ";

    public Database(string connectionString)
    {
        Connection = new OracleConnection(connectionString);
    }

    IDbConnection Connection { get; }

    public void Dispose()
    {
        Connection?.Dispose();
    }

    public async Task EnsureTablesExist()
    {
        var result = await Connection.QueryAsync(@"
            select table_name
            from user_tables
            where table_name = 'ULS_LIBINSIGHT_INST_RECORDS'
        ");
        if (!result.Any())
        {
            await Connection.ExecuteAsync(TableCreationSql);
        }
    }

    public async Task<bool> RecordExistsInDb(int recordId)
    {
        var records = await Connection.QueryAsync(
            "select RecordId from ULS_LIBINSIGHT_INST_RECORDS where RecordId = :recordId",
            new { recordId });
        return records.Any();
    }

    public async Task UpdateRecord(JObject record)
    {
        await Connection.ExecuteAsync(UpdateSql, ToParam(record));
        var recordId = (int)record["_id"];
        foreach (var field in MultiselectFields)
        {
            // Get the values already in the db, and compare with the values in the record to see if there is a difference.
            var valuesInDb = (await Connection.QueryAsync<string>(@$"
                select {field.ColumnName} from {field.TableName}
                where RecordId = :recordId
            ", new { recordId })).ToList();
            var valuesInRecord = (record[field.FieldName] as JArray)?.Select(CleanString)?.Where(value => value is not null)?.ToList() ?? new List<string>();
            // In the case that there is no change, both these lists will be empty.
            var valuesToBeInserted = valuesInRecord.Except(valuesInDb).ToList();
            var valuesToBeRemoved = valuesInDb.Except(valuesInRecord).ToList();
            if (valuesToBeRemoved.Any())
            {
                // Dapper expands the list into individual parameters for each element, so this could fail if that goes over the oracle max parameters.
                // That will probably never happen, though.
                await Connection.ExecuteAsync(@$"
                    delete from {field.TableName}
                    where RecordId = :recordId
                    and {field.ColumnName} in :valuesToBeRemoved
                ", new { recordId, valuesToBeRemoved });
            }
            if (valuesToBeInserted.Any())
            {
                await Connection.ExecuteAsync(@$"
                    insert into {field.TableName} (RecordId, {field.ColumnName})
                    values (:RecordId, :{field.ColumnName})
                ", valuesToBeInserted.Select(value => new Dictionary<string, object>
                {
                    ["RecordId"] = recordId,
                    [field.ColumnName] = value
                }));
            }
        }
    }

    public async Task InsertRecord(JObject record)
    {
        await Connection.ExecuteAsync(InsertSql, ToParam(record));
        foreach (var field in MultiselectFields)
        {
            if (record[field.FieldName] is JArray array)
            {
                await Connection.ExecuteAsync(@$"
                    insert into {field.TableName} (RecordId, {field.ColumnName})
                    values (:RecordId, :{field.ColumnName})
                ", array.Select(CleanString).Where(value => value is not null).Select(value => new Dictionary<string, object>
                {
                    ["RecordId"] = (int)record["_id"],
                    [field.ColumnName] = value
                }));
            }
        }
    }

    record MultiselectFieldData(string FieldName, string TableName, string ColumnName);

    List<MultiselectFieldData> MultiselectFields = new List<MultiselectFieldData> {
        new MultiselectFieldData("Topics covered", "ULS_LIBINSIGHT_INST_TOPICS_COVERED", "TopicsCovered"),
        new MultiselectFieldData("Method of delivery", "ULS_LIBINSIGHT_INST_METHOD_OF_DELIVERY", "MethodOfDelivery"),
        new MultiselectFieldData("Audience", "ULS_LIBINSIGHT_INST_AUDIENCE", "Audience"),
        new MultiselectFieldData("Skills taught", "ULS_LIBINSIGHT_INST_SKILLS_TAUGHT", "SkillsTaught"),
        new MultiselectFieldData("Tools discussed", "ULS_LIBINSIGHT_INST_TOOLS_DISCUSSED", "ToolsDiscussed"),
        new MultiselectFieldData("Teaching Consultation Results", "ULS_LIBINSIGHT_INST_TEACHING_CONSULTATION_RESULTS", "TeachingConsultationResults"),
    };

    // If the field definitions for the dataset are ever changed, they would need to be updated in ToParam and MultiselectFields.
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

    static string CleanString(JToken input)
    {
        var s = input?.ToString();
        return string.IsNullOrWhiteSpace(s) ? null : s.Trim().Replace("'Äô", "'").Replace("’", "'");
    }

    static object NumberOrNull(JToken input) =>
        input?.Type switch
        {
            JTokenType.Integer => (int?)input,
            JTokenType.Float => (double?)input,
            _ => null
        };

    static string ArraySingleElement(JToken input)
    {
        if (input is JArray array)
        {
            return CleanString(array.Single());
        }
        else return null;
    }
}