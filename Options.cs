using System;
using CommandLine;

class Options 
{
    [Option('f', "from", Required = false, HelpText = "From date, defaults to beginning of FY (the previous July 1)")]
    public DateTime? FromDate { get; set; }
    [Option('t', "to", Required = false, HelpText = "To date, defaults to today")]
    public DateTime? ToDate { get; set; }
    [Value(0)]
    public DatasetId DatasetId { get; set; }
}

enum DatasetId
{
    InstructionOutreach = 29168,
    HillHeadCounts = 31294,
}