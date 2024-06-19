
# CFB DuckDB

This repo is an example of how to use dlt to create a data pipeline from an API and store the output to a database. It is configured to save to a DuckDB file which is available in the repo. It also includes a sample GitHub Action script for both backfilling the entire database from Princeton Rutgers 1869-Present as well as a weekly update that refreshes the current season.

## Data Source

The data in this repo is sourced from [collegefootballdata.com](https://collegefootballdata.com). API Keys are available for free. While this project currently covers 24 of the available endpoints, it does not include the endpoints that require a specific game id (though if you want to add that, please feel free!)

## Setup

To get started, clone this repo and in a virtual environment run `pip install -r requirements.txt`.

You will also need to create a file called `settings.py` in the root of your cloned repo and add `API_KEY='Bearer <YOUR_KEY_HERE>'` as the contents of the file. If you are running a GitHub Action on your clone, add a repository secret named `CFBD_API_KEY` and the existing action config file will create the `settings.py` file at run time.

To run the pipeline locally - from the root folder run `PROGRESS=enlighten python cfb_data.py` to refresh the current season. To backfill the entire db, run `PROGRESS=enlighten python cfb_data.py backfill`.

## Usage

If you just want to query this data, you can attach to the duckdb file in the repo. You can do this use dbeaver or another SQL IDE of your choice (or R or Python etc using standard libraries). Simply clone the main branch of the repo and you'll have an up to date duckdb file that can be used locally without having to rerun any API requests. Many dataviz tools also support DuckDB and you can upload that as well if you want to build out dashboards.

Alternatively, you can easily modify this repo to use [MotherDuck](https://motherduck.com) - a cloud version of DuckDB that gives 10GB of storage and 10 hours of query time a month for free.

