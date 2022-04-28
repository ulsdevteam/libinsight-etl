using System;
using CommandLine;

class Options 
{
    [Option('f', "from", Required = false, HelpText = "From date, defaults to beginning of FY (the previous July 1)")]
    public DateTime? FromDate { get; set; }
    [Option('t', "to", Required = false, HelpText = "To date, defaults to today")]
    public DateTime? ToDate { get; set; }
}