using System.Data;
using Dapper;
using Newtonsoft.Json.Linq;

class HeadCountsDataset : Dataset
{
    public HeadCountsDataset(IDbConnection connection) : base(connection) { }

    public override int DatasetId => 31294;
    public override int RequestId => 20;

    public override async Task EnsureTablesExist()
    {
        var exists = (await Connection.QueryAsync(@"
            select table_name from user_tables
            where table_name = 'ULS_LIBINSIGHT_HILL_HEADCOUNTS'
        ")).Any();
        if (exists) return;
        await Connection.ExecuteAsync(@"
            create table ULS_LIBINSIGHT_HILL_HEADCOUNTS
            (
                RecordId number not null,
                StartDate date not null,
                EnteredBy varchar2(4000) not null,
                Floor varchar2(4000) null,
                TimeOfHeadCount varchar2(4000) null,
                NumberOfPatrons number null,
                DeskInteractionDateTime date null,
                TransactionsInHour number null,
                primary key (RecordId)
            );
        ");
    }

    public override async Task InsertRecord(JObject record)
    {
        await Connection.ExecuteAsync(@"
            insert into ULS_LIBINSIGHT_HILL_HEADCOUNTS
            (
                RecordId,
                StartDate,
                EnteredBy,
                Floor,
                TimeOfHeadCount,
                NumberOfPatrons,
                DeskInteractionDateTime,
                TransactionsInHour
            )
            values
            (
                :RecordId,
                :StartDate,
                :EnteredBy,
                :Floor,
                :TimeOfHeadCount,
                :NumberOfPatrons,
                :DeskInteractionDateTime,
                :TransactionsInHour
            )
        ", ToParam(record));
    }

    public override async Task<bool> RecordExistsInDb(int recordId)
    {
        return (await Connection.QueryAsync(@"
            select RecordId from ULS_LIBINSIGHT_HILL_HEADCOUNTS
            where RecordId = :recordId
        ", new { recordId })).Any();
    }

    public override async Task UpdateRecord(JObject record)
    {
        await Connection.ExecuteAsync(@"
            update ULS_LIBINSIGHT_HILL_HEADCOUNTS set
                StartDate = :StartDate,
                EnteredBy = :EnteredBy,
                Floor = :Floor,
                TimeOfHeadCount = :TimeOfHeadCount,
                NumberOfPatrons = :NumberOfPatrons,
                DeskInteractionDateTime = :DeskInteractionDateTime,
                TransactionsInHour = :TransactionsInHour
            where RecordId = :RecordId
        ", ToParam(record));
    }

    static object ToParam(JObject record) => new {
        RecordId = (int)record["_id"],
        StartDate = (DateTime?)record["_start_date"],
        EnteredBy = (string)record["_entered_by"],
        Floor = ArraySingleElement(record["Floor"]),
        TimeOfHeadCount = ArraySingleElement(record["Time of Head Count"]),
        NumberOfPatrons = NumberOrNull(record["Number of Patrons"]),
        DeskInteractionDateTime = DateTimeOrNull(record["Date and Time for Desk Interactions"]),
        TransactionsInHour = NumberOrNull(record["Number of transactions in the hour"])
    };
}