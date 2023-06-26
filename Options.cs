using System;
using CommandLine;
using CommandLine.Text;

class Options 
{
    [Option('f', "from", Required = false, HelpText = "From date, defaults to beginning of FY (the previous July 1)")]
    public DateTime? FromDate { get; set; }
    [Option('t', "to", Required = false, HelpText = "To date, defaults to today")]
    public DateTime? ToDate { get; set; }
    [Value(0, MetaName = "dataset", Required = true, HelpText = "Which dataset to update (not case sensitive)")]
    public DatasetId DatasetId { get; set; }

    [Usage]
    public static IEnumerable<Example> Examples {get;} = new List<Example> {
        new Example("Update Instruction & Outreach dataset", new Options{ DatasetId = DatasetId.InstructionOutreach }),
        new Example("Update Hillman Head Counts dataset", new Options { DatasetId = DatasetId.HillHeadCounts })
    };
}

enum DatasetId
{
    InstructionOutreach = 29168,
    HillHeadCounts = 31377,
}