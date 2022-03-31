using dotenv.net;
using CommandLine;
using Microsoft.Extensions.Configuration;

DotEnv.Load();
IConfiguration Config = new ConfigurationBuilder().AddEnvironmentVariables().Build();
await CommandLine.Parser.Default.ParseArguments<Options>(args).WithParsedAsync(async options =>
{
    var libInsightClient = new LibInsightClient();
    await libInsightClient.Authorize(Config["LIBINSIGHT_CLIENT_ID"], Config["LIBINSIGHT_CLIENT_SECRET"]);
    var records = await libInsightClient.GetRecords(29168, 19, options.FromDate, options.ToDate ?? DateTime.Today);
    using var db = new Database(Config["ORACLE_CONNECTION_STRING"]);
    await db.EnsureTablesExist();
    foreach (var record in records)
    {
        await db.InsertRecord(record);
    }
});
