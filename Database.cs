using Newtonsoft.Json.Linq;
using Dapper;
using System.Data;
using Oracle.ManagedDataAccess.Client;

class Database : IDisposable
{
    string TableCreationSql = @"
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
        await Connection.ExecuteAsync(@"
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
        ", ToParam(record));
        // TODO: handling multiselect fields
    }

    public async Task InsertRecord(JObject record)
    {
        var recordId = (int)record["_id"];
        await Connection.ExecuteAsync(@"
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
        ", ToParam(record));
        if (record["Topics covered"] is JArray topics)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INST_TOPICS_COVERED (RecordId, TopicsCovered)
                values (:recordId, :TopicsCovered)
            ", topics.Select(CleanString).Where(x => x is not null).Select(x => new { recordId, TopicsCovered = x }));
        }

        if (record["Method of delivery"] is JArray methods)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INST_METHOD_OF_DELIVERY (RecordId, MethodOfDelivery)
                values (:recordId, :MethodOfDelivery)
            ",
                methods.Select(CleanString).Where(x => x is not null)
                    .Select(x => new { recordId, MethodOfDelivery = x }));
        }

        if (record["Audience"] is JArray audience)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INST_AUDIENCE (RecordId, Audience)
                values (:recordId, :Audience)
            ", audience.Select(CleanString).Where(x => x is not null).Select(x => new { recordId, Audience = x }));
        }

        if (record["Skills taught"] is JArray skills)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INST_SKILLS_TAUGHT (RecordId, SkillsTaught)
                values (:recordId, :SkillsTaught)
            ", skills.Select(CleanString).Where(x => x is not null).Select(x => new { recordId, SkillsTaught = x }));
        }

        if (record["Tools discussed"] is JArray tools)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INST_TOOLS_DISCUSSED (RecordId, ToolsDiscussed)
                values (:recordId, :ToolsDiscussed)
            ", tools.Select(CleanString).Where(x => x is not null).Select(x => new { recordId, ToolsDiscussed = x }));
        }

        if (record["Teaching Consultation Results"] is JArray consultation)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INST_TEACHING_CONSULTATION_RESULTS (RecordId, TeachingConsultationResults)
                values (:recordId, :TeachingConsultationResults)
            ",
                consultation.Select(CleanString).Where(x => x is not null)
                    .Select(x => new { recordId, TeachingConsultationResults = x }));
        }
    }

    static object ToParam(JObject record) => new
    {
        RecordId = (int)record["_id"],
        StartDate = (DateTime?)record["_start_date"],
        EnteredBy = (string?)record["_entered_by"],
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

    static string? CleanString(JToken? input)
    {
        var s = input?.ToString();
        return string.IsNullOrWhiteSpace(s) ? null : s.Trim().Replace("'Äô", "'").Replace("’", "'");
    }

    static object? NumberOrNull(JToken? input) =>
        input?.Type switch
        {
            JTokenType.Integer => (int?)input,
            JTokenType.Float => (double?)input,
            _ => null
        };

    static string? ArraySingleElement(JToken? input)
    {
        if (input is JArray array)
        {
            return CleanString(array.Single());
        }
        else return null;
    }
}