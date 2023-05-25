# LibInsight ETL

This console application pulls data from the Instruction and Outreach dataset in LibInsight, cleans it, and inserts it into the data warehouse.

This will run once at the beginning of each month. When it runs in July and the default from date is used, it will pull data going back to July of the previous year.

## Usage

    libinsight-etl 2.0.0
    Copyright (C) 2023 University of Pittsburgh
    USAGE:
    Update Instruction & Outreach dataset:
    libinsight-etl InstructionOutreach
    Update Hillman Head Counts dataset:
    libinsight-etl HillHeadCounts

    -f, --from          From date, defaults to beginning of FY (the previous July 1)

    -t, --to            To date, defaults to today

    --help              Display this help screen.

    --version           Display version information.

    dataset (pos. 0)    Required. Which dataset to update (not case sensitive)

## Adding a new dataset

In LibInsight, navigate to Admin > Widgets and API. The table here displays the request id and dataset id used for the API call.

## Building

Building a Linux executable on Windows:
`dotnet publish -r linux-x64 -p:PublishSingleFile=true --self-contained true`