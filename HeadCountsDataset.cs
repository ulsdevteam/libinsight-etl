using System.Data;
using System.Globalization;
using Snowflake.Data.Client;
using Dapper;
using Newtonsoft.Json.Linq;

class HeadCountsDataset : Dataset
{
    public HeadCountsDataset(SnowflakeDbConnection connection, LibInsightClient client) : base(connection, client) { }

    public override int DatasetId => 31377;
    public override int RequestId => 21;

    public override async Task ProcessDateRange(DateTime fromDate, DateTime toDate)
    {
        await EnsureTablesExist();
        var records = new List<DynamicParameters>();
        foreach (var (weekStart, weekEnd) in DateIntervals(fromDate, toDate, 7))
        {
            var data = await LibInsightClient.GetGateCountData(DatasetId, weekStart, weekEnd, "hourly");
            if (data["hourly"] is JObject hourlyData)
            {
                foreach (var (timestamp, counts) in hourlyData)
                {
                    var recordTime = DateTime.ParseExact(timestamp, "yyyy-MM-dd htt", CultureInfo.InvariantCulture);
                    foreach (var (locationId, count) in counts as JObject)
                    {
                        var p = new DynamicParameters();
                        p.Add("1", recordTime);
                        p.Add("2", int.Parse(locationId));
                        p.Add("3", data["libraries"][locationId].ToString());
                        p.Add("4", (int) count);
                        records.Add(p);
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
        await Connection.ExecuteAsync(@"
            create table if not exists LIBINSIGHT_HILL_HEADCOUNTS
            (
                RecordTime timestamp not null,
                LocationId number not null,
                Location varchar not null,
                TransactionCount number not null,
                primary key (RecordTime, LocationId)
            );
        ");
    }

    async Task UpsertRecords(IEnumerable<DynamicParameters> records)
    {
        await Connection.ExecuteAsync(@"
            MERGE INTO LIBINSIGHT_HILL_HEADCOUNTS AS TARGET
            USING (
                VALUES (?, ?, ?, ?)
            ) AS SOURCE (RecordTime, LocationId, Location, TransactionCount)
            ON SOURCE.RecordTime = TARGET.RecordTime AND SOURCE.LocationId = TARGET.LocationId
            WHEN MATCHED THEN
                UPDATE SET TransactionCount = SOURCE.TransactionCount
            WHEN NOT MATCHED THEN
                INSERT (RecordTime, LocationId, Location, TransactionCount)
                VALUES (SOURCE.RecordTime, SOURCE.LocationId, SOURCE.Location, SOURCE.TransactionCount);
        ", records);
    }
}