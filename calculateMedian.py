import pandas as pd
import statistics
import numpy as np

def calculateMedian(filename):
    df = pd.read_csv(filename)
    weeks = range(1,54)
    median = []
    for week in weeks:
        data = df['week'+str(week)]
        values = [x for x in data.values if x != 0]
        print(values)
        if len(values) > 0:
            median.append(statistics.median(values))

    headers = [f'week{i}' for i in range(1, 1+len(median))]
    return pd.DataFrame(np.array(median).reshape(-1, len(median)), columns=headers, index=None)

if __name__ == "__main__":
    res = calculateMedian("trip_miles_2015.csv")
    res.to_csv("trip_miles_median_2015.csv", index=False)
        

