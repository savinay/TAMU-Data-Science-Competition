# pylint: disable=missing-docstring, invalid-name
"""Get weekly/daily/hourly total sums for a given timechunk."""

import os
import pandas as pd


if __name__ == "__main__":
    timechunk = "weekly"
    for year in range(2013, 2018):
    # for year in range(2017, 2018):
        sumsfolderpath = f"{timechunk}/sums/{year}"
        totals = pd.DataFrame()
        for infile in os.listdir(sumsfolderpath):
            colname = infile.split(f"_{year}")[0]
            totals[colname] = pd.read_csv(f"{sumsfolderpath}/{infile}").sum()
        totals.to_csv(f"{timechunk}/totals/total_{year}.csv")
