using Newtonsoft.Json.Linq;
using Dapper;
using System.Data;
using Oracle.ManagedDataAccess.Client;

class Database : IDisposable
{
    IDbConnection Connection { get; }

    public Database(string connectionString)
    {
        Connection = new OracleConnection(connectionString);
    }

    string TableCreationSql = @"
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH
        (
            RecordId number not null,
            StartDate date not null,
            EnteredBy varchar2(4000) not null,
            EventName varchar2(4000) null,
            FacultySponsorName varchar2(4000) null,
            FacultySponsorEmail varchar2(4000) null,
            Department varchar2(4000) null,
            NumberOfParticipants number not null,
            DurationOfEvent number not null,
            CoInstructorsOrganisation varchar2(4000) null,
            Notes varchar2(4000) null,
            LocationOfEvent varchar2(4000) not null,
            LocationOther varchar2(4000) null,
            EventType varchar2(4000) not null,
            ClassNumber number null,
            AdditionalMinutes number null,
            EDI varchar2(4000) null,
            primary key (RecordId)
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TOPICS_COVERED
        (
            RecordId number not null,
            TopicsCovered varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_METHOD_OF_DELIVERY
        (
            RecordId number not null,
            MethodOfDelivery varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_AUDIENCE
        (
            RecordId number not null,
            Audience varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_SKILLS_TAUGHT
        (
            RecordId number not null,
            SkillsTaught varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TOOLS_DISCUSSED
        (
            RecordId number not null,
            ToolsDiscussed varchar2(4000) not null
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TEACHING_CONSULTATION_RESULTS
        (
            RecordId number not null,
            TeachingConsultationResults varchar2(4000) not null
        );
    ";

    public async Task EnsureTablesExist()
    {
        var result = await Connection.QueryAsync(@"
            select table_name
            from user_tables
            where table_name = 'ULS_LIBINSIGHT_INSTRUCTION_OUTREACH'
        ");
        if (!result.Any())
        {
            await Connection.ExecuteAsync(TableCreationSql);
        }
    }

    public async Task InsertRecord(JObject record)
    {
        var RecordId = (int?)record["_id"];
        // TODO: what to do when a record already exists in db. ignore or update?
        await Connection.ExecuteAsync(@"
            insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH
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
        ", new
        {
            RecordId,
            StartDate = (DateTime?)record["_start_date"],
            EnteredBy = (string?)record["_entered_by"],
            EventName = CleanString(record["Event Name (if a class, search for course title and number  here) "]),
            FacultySponsorName = CleanString(record["Faculty/ Sponsor Name"]),
            FacultySponsorEmail = CleanString(record["Faculty/ Sponsor Email"]),
            Department = ArraySingleElement(record["Department"]),
            NumberOfParticipants = (int?)record["Number of Participants"],
            DurationOfEvent = (int?)record["Duration of Event"],
            CoInstructorsOrganisation = CleanString(record["Co-Instructor(s)/ Organisation"]),
            Notes = CleanString(record["Notes"]),
            LocationOfEvent = ArraySingleElement(record["Location of Event"]),
            LocationOther = CleanString(record["Location - Other"]),
            EventType = ArraySingleElement(record["Event Type"]),
            ClassNumber = (int?)record["Class Number (5 digits) Available at  Class Search"],
            AdditionalMinutes = (int?)record["Additional minutes of prep/follow-up"],
            EDI = ArraySingleElement(record["Equity, Diversity, Inclusion (EDI)"]),
        });
        if (record["Topics covered"] is JArray topics)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TOPICS_COVERED (RecordId, TopicsCovered)
                values (:RecordId, :TopicsCovered)
            ", topics.Select(CleanString).Where(x => x is not null).Select(x => new { RecordId, TopicsCovered = x }));
        }
        if (record["Method of delivery"] is JArray methods)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_METHOD_OF_DELIVERY (RecordId, MethodOfDelivery)
                values (:RecordId, :MethodOfDelivery)
            ", methods.Select(CleanString).Where(x => x is not null).Select(x => new { RecordId, MethodOfDelivery = x }));
        }
        if (record["Audience"] is JArray audience)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_AUDIENCE (RecordId, Audience)
                values (:RecordId, :Audience)
            ", audience.Select(CleanString).Where(x => x is not null).Select(x => new { RecordId, Audience = x }));
        }
        if (record["Skills taught"] is JArray skills)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_SKILLS_TAUGHT (RecordId, SkillsTaught)
                values (:RecordId, :SkillsTaught)
            ", skills.Select(CleanString).Where(x => x is not null).Select(x => new { RecordId, SkillsTaught = x }));
        }
        if (record["Tools discussed"] is JArray tools)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TOOLS_DISCUSSED (RecordId, ToolsDiscussed)
                values (:RecordId, :ToolsDiscussed)
            ", tools.Select(CleanString).Where(x => x is not null).Select(x => new { RecordId, ToolsDiscussed = x }));
        }
        if (record["Teaching Consultation Results"] is JArray consultation)
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TEACHING_CONSULTATION_RESULTS (RecordId, TeachingConsultationResults)
                values (:RecordId, :TeachingConsultationResults)
            ", consultation.Select(CleanString).Where(x => x is not null).Select(x => new { RecordId, TeachingConsultationResults = x }));
        }
    }

    string? CleanString(JToken? input)
    {
        var s = input?.ToString();
        if (string.IsNullOrWhiteSpace(s))
        {
            return null;
        }
        else
        {
            return s.Replace("'Äô", "'");
        }
    }

    string? ArraySingleElement(JToken? input)
    {
        if (input is JArray array)
        {
            return CleanString(array.Single().ToString());
        }
        else return null;
    }

    public void Dispose()
    {
        Connection?.Dispose();
    }
}