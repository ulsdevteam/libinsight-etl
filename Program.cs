using dotenv.net;
using CommandLine;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;

DotEnv.Load();
IConfiguration Config = new ConfigurationBuilder().AddEnvironmentVariables().Build();
await CommandLine.Parser.Default.ParseArguments<Options>(args).WithParsedAsync(async options =>
{
    try
    {
        var libInsightClient = new LibInsightClient();
        await libInsightClient.Authorize(Config["LIBINSIGHT_CLIENT_ID"], Config["LIBINSIGHT_CLIENT_SECRET"]);
        var records = await libInsightClient.GetRecords(29168, 19, options.FromDate ?? StartOfFiscalYear(), options.ToDate ?? DateTime.Today);
        using var db = new Database(Config["ORACLE_CONNECTION_STRING"]);
        await db.EnsureTablesExist();
        foreach (var record in records)
        {
            try
            {
                var recordId = (int?)record["_id"];
                if (recordId is null)
                {
                    Console.Error.WriteLine("Record is missing an Id.");
                }
                else if (await db.RecordExistsInDb(recordId.Value))
                {
                    await db.UpdateRecord(record);
                }
                else
                {
                    await db.InsertRecord(record);
                }
            }
            catch (OracleException exception)
            {
                Console.Error.WriteLine(exception);
            }
        }
    }
    catch (Exception exception)
    {
        Console.Error.WriteLine(exception);
        throw;
    }
});

DateTime StartOfFiscalYear() => new DateTime(DateTime.Today.Month >= 7 ? DateTime.Today.Year : DateTime.Today.Year - 1, 7, 1);
