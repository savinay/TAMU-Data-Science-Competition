# pylint: skip-file
import pandas as pd
import statistics
import numpy as np
import os

def calculateMedian(filename, median, res_file):
    df = pd.read_csv(filename)
    weeks = range(1,54)
    for week in weeks:
        data = df['week'+str(week)]
        values = [x for x in data.values if x != 0]
        key = res_file.split("_")[1]
        if key not in median:
            median[key] = []
        if len(values) > 0:
            median[key].append(statistics.median(values))
        else:
            median[key].append(0)
    
    return median


if __name__ == "__main__":
    for file in os.listdir("Predictors"):
        if file != ".DS_Store" and file != "allYears_uniqueTaxi.csv.xlsx":
            try:
                fi = os.listdir("Predictors/"+file)
                median = {}
                year = fi[0].split("_")[0]
                for f in fi:
                    res_file = "_".join(f.split("_")[0:2]) + "_median"
                    median = calculateMedian("Predictors/"+file+"/"+f, median, res_file)
                headers = ['Indicator', *[f'week{i}' for i in range(1, 54)]]
                result = pd.DataFrame(
                    [[key, *val] for key, val in median.items()], columns=headers, index=None)
                result.to_csv(f"{year}_median.csv", index=False)
            except:
                continue