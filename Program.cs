using dotenv.net;
using CommandLine;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;

DotEnv.Load();
IConfiguration config = new ConfigurationBuilder().AddEnvironmentVariables().Build();
var parser = new Parser(settings =>
{
    settings.AutoHelp = true;
    settings.CaseInsensitiveEnumValues = true;
    settings.HelpWriter = Console.Error;
});
await parser.ParseArguments<Options>(args).WithParsedAsync(async options =>
{
    try
    {
        var libInsightClient = new LibInsightClient();
        await libInsightClient.Authorize(config["LIBINSIGHT_CLIENT_ID"], config["LIBINSIGHT_CLIENT_SECRET"]);
        using var conn = new OracleConnection(config["ORACLE_CONNECTION_STRING"]);
        Dataset dataset = options.DatasetId switch {
            DatasetId.InstructionOutreach => new InstructionOutreachDataset(conn),
            DatasetId.HillHeadCounts => new HeadCountsDataset(conn),
            _ => throw new Exception("Dataset not recognized"),
        };
        await dataset.EnsureTablesExist();
        var records = await libInsightClient.GetRecords(dataset.DatasetId, dataset.RequestId, options.FromDate ?? StartOfFiscalYear(), options.ToDate ?? DateTime.Today);
        foreach (var record in records)
        {
            try
            {
                var recordId = (int?)record["_id"];
                if (recordId is null)
                {
                    Console.Error.WriteLine("Record is missing an Id.");
                }
                else if (await dataset.RecordExistsInDb(recordId.Value))
                {
                    await dataset.UpdateRecord(record);
                }
                else
                {
                    await dataset.InsertRecord(record);
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

// Returns the first day of the previous July
DateTime StartOfFiscalYear() => new DateTime(DateTime.Today.Month > 7 ? DateTime.Today.Year : DateTime.Today.Year - 1, 7, 1);
