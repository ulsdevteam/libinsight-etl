using System.Data;
using System.Globalization;
using Dapper;
using Newtonsoft.Json.Linq;

class HeadCountsDataset : Dataset
{
    public HeadCountsDataset(IDbConnection connection, LibInsightClient client) : base(connection, client) { }

    public override int DatasetId => 31377;
    public override int RequestId => 21;

    public override async Task ProcessDateRange(DateTime fromDate, DateTime toDate)
    {
        await EnsureTablesExist();
        var records = new List<object>();
        foreach (var (weekStart, weekEnd) in DateIntervals(fromDate, toDate, 7))
        {
            var data = await LibInsightClient.GetGateCountData(DatasetId, RequestId, weekStart, weekEnd, "hourly");
            if (data["hourly"] is JObject hourlyData)
            {
                foreach (var (timestamp, counts) in hourlyData)
                {
                    var recordTime = DateTime.ParseExact(timestamp, "yyyy-MM-dd htt", CultureInfo.InvariantCulture);
                    foreach (var (locationId, count) in counts as JObject)
                    {
                        records.Add(new
                        {
                            recordTime,
                            locationId = int.Parse(locationId),
                            location = data["libraries"][locationId].ToString(),
                            transactionCount = (int) count
                        });
                    }
                }
            }
        }
        await UpsertRecords(records);
    }

    static IEnumerable<(DateTime, DateTime)> DateIntervals(DateTime start, DateTime end, int intervalLengthInDays)
    {
        var currentStart = start;
        DateTime nextStart;
        while ((nextStart = currentStart.AddDays(intervalLengthInDays)) < end)
        {
            yield return (currentStart, nextStart);
            currentStart = nextStart;
        }
        yield return (currentStart, end);
    }

    async Task EnsureTablesExist()
    {
        var exists = (await Connection.QueryAsync(@"
            select table_name from user_tables
            where table_name = 'ULS_LIBINSIGHT_HILL_HEADCOUNTS'
        ")).Any();
        if (exists) return;
        await Connection.ExecuteAsync(@"
            create table ULS_LIBINSIGHT_HILL_HEADCOUNTS
            (
                RecordTime date not null,
                LocationId number not null,
                Location varchar2(4000) not null,
                TransactionCount number not null,
                primary key (RecordTime, LocationId)
            );
        ");
    }

    async Task UpsertRecords(IEnumerable<object> records)
    {
        await Connection.ExecuteAsync(@"
            begin
                insert into ULS_LIBINSIGHT_HILL_HEADCOUNTS
                (
                    RecordTime,
                    LocationId,
                    Location,
                    TransactionCount
                )
                values
                (
                    :recordTime,
                    :locationId,
                    :location,
                    :transactionCount
                );
            exception when dup_val_on_index then
                update ULS_LIBINSIGHT_HILL_HEADCOUNTS set
                    TransactionCount = :transactionCount
                where
                    RecordTime = :recordTime and
                    LocationId = :locationId;
            end;
        ", records);
    }
}