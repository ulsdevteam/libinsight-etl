using System;
using CommandLine;

class Options 
{
    [Option('f', "from", Required = false)]
    public DateTime? FromDate { get; set; }
    [Option('t', "to", Required = false)]
    public DateTime? ToDate { get; set; }
}