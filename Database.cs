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
            Id number,
            StartDate date,
            EnteredBy varchar2(4000),
            EventName varchar2(4000),
            FacultySponsorName varchar2(4000),
            FacultySponsorEmail varchar2(4000),
            Department varchar2(4000),
            NumberOfParticipants number,
            DurationOfEvent number,
            CoInstructorsOrganisation varchar2(4000),
            Notes varchar2(4000),
            LocationOfEvent varchar2(4000),
            LocationOther varchar2(4000),
            EventType varchar2(4000),
            ClassNumber number,
            AdditionalMinutes number,
            EDI varchar2(4000),
            primary key (Id)
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TOPICS_COVERED
        (
            Id number,
            TopicsCovered varchar2(4000)
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_METHOD_OF_DELIVERY
        (
            Id number,
            MethodOfDelivery varchar2(4000)
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_AUDIENCE
        (
            Id number,
            Audience varchar2(4000)
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_SKILLS_TAUGHT
        (
            Id number,
            SkillsTaught varchar2(4000)
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TOOLS_DISCUSSED
        (
            Id number,
            ToolsDiscussed varchar2(4000)
        );
        create table ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TEACHING_CONSULTATION_RESULTS
        (
            Id number,
            TeachingConsultationResults varchar2(4000)
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
        var Id = (int?)record["_id"];
        // TODO: what to do when a record already exists in db. ignore or update?
        await Connection.ExecuteAsync(@"
            insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH
            (
                Id,
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
                :Id,
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
            Id,
            StartDate = (DateTime?)record["_start_date"],
            EnteredBy = (string?)record["_entered_by"],
            EventName = CleanString(record["Event Name (if a class, search for course title and number  here) "]),
            FacultySponsorName = CleanString(record["Faculty/ Sponsor Name"]),
            FacultySponsorEmail = CleanString(record["Faculty/ Sponsor Email"]),
            Department = ArraySingleElement(record["Department"]),
            NumberOfParticipants = (int?)record["Number of Participants"],
            DurationOfEvent = (int?)record["Duration of Event"],
            CoInstructorsOrganization = CleanString(record["Co-Instructor(s)/ Organisation"]),
            Notes = CleanString(record["Notes"]),
            LocationOfEvent = ArraySingleElement(record["Location of Event"]),
            LocationOther = CleanString(record["Location - Other"]),
            EventType = ArraySingleElement(record["Event Type"]),
            ClassNumber = (int?)record["Class Number (5 digits) Available at  Class Search"],
            AdditionalMinutes = (int?)record["Additional minutes of prep/follow-up"],
            EDI = ArraySingleElement(record["Equity, Diversity, Inclusion (EDI)"]),
        });
        if (record["Topics covered"] is JArray topics && topics.Any())
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TOPICS_COVERED (Id, TopicsCovered)
                values (:Id, :TopicsCovered)
            ", topics.Select(x => new { Id, TopicsCovered = CleanString(x) }));
        }
        if (record["Method of delivery"] is JArray methods && methods.Any())
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_METHOD_OF_DELIVERY (Id, MethodOfDelivery)
                values (:Id, :MethodOfDelivery)
            ", methods.Select(x => new { Id, MethodOfDelivery = CleanString(x) }));
        }
        if (record["Audience"] is JArray audience && audience.Any())
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_AUDIENCE (Id, Audience)
                values (:Id, :Audience)
            ", audience.Select(x => new { Id, Audience = CleanString(x) }));
        }
        if (record["Skills taught"] is JArray skills && skills.Any())
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_SKILLS_TAUGHT (Id, SkillsTaught)
                values (:Id, :SkillsTaught)
            ", skills.Select(x => new { Id, SkillsTaught = CleanString(x) }));
        }
        if (record["Tools discussed"] is JArray tools && tools.Any())
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TOOLS_DISCUSSED (Id, ToolsDiscussed)
                values (:Id, :ToolsDiscussed)
            ", tools.Select(x => new { Id, ToolsDiscussed = CleanString(x) }));
        }
        if (record["Teaching Consultation Results"] is JArray consultation && consultation.Any())
        {
            await Connection.ExecuteAsync(@"
                insert into ULS_LIBINSIGHT_INSTRUCTION_OUTREACH_TEACHING_CONSULTATION_RESULTS (Id, TeachingConsultationResults)
                values (:Id, :TeachingConsulationResults)
            ", consultation.Select(x => new { Id, TeachingConsultationResults = CleanString(x) }));
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