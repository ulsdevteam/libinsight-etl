# LibInsight Instruction & Outreach Dataset ETL

This console application pulls data from the Instruction and Outreach dataset in LibInsight, cleans it, and inserts it into the data warehouse.

This will run once at the beginning of each month. When it runs in July and the default from date is used, it will pull data going back to July of the previous year.

## Usage

    libinsight-instruction-outreach-etl 1.0.0
    Copyright (C) 2022 University of Pittsburgh

    -f, --from    From date, defaults to beginning of FY (the previous July 1)

    -t, --to      To date, defaults to today

    --help        Display this help screen.

    --version     Display version information.

## Building

Building a Linux executable on Windows:
`dotnet publish -r linux-x64 -p:PublishSingleFile=true --self-contained true`