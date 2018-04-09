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
        if len(values) > 0:
            median.append(statistics.median(values))

    headers = [f'week{i}' for i in range(1, 1+len(median))]
    return pd.DataFrame(np.array(median).reshape(-1, len(median)), columns=headers, index=None)

if __name__ == "__main__":
    for i in range(3, 8):
        res = calculateMedian("out.csv")
        res.to_csv(f"out_median_201{i}.csv", index=False)
        

