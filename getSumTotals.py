# pylint: disable=missing-docstring, invalid-name
"""Get weekly/daily/hourly sums per taxi and medians across all taxis
of things like trip totals and trip seconds."""

import os
import pandas as pd


if __name__ == "__main__":
    timechunk = "daily"
    for year in range(2013, 2018):
        sumsfolderpath = f"{timechunk}/sums/{year}"
        for infile in os.listdir(sumsfolderpath):
            pd.read_csv(infile).sum().to_csv(f"{timechunk}/totals/{infile}")

