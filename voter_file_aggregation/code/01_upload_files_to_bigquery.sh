#!/bin/bash

bq mk voter_records
bq load --field_delimiter="\t" --skip_leading_rows=1 voter_records.records_table data/cleaned_voter_files/rhode_island.tsv data/bigquery_schema.json
