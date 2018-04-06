import pandas as pd
import statistics
import numpy as np

def calculateMedian():
    df = pd.read_csv("out.csv")
    weeks = range(1,54)
    median = []
    for week in weeks:
        data = df['week'+str(week)]
        median.append(statistics.median(data.values))
    headers = [f'week{i}' for i in range(1, 54)]
    return pd.DataFrame(np.array(median).reshape(-1, len(median)), columns=headers, index=None)

if __name__ == "__main__":
    res = calculateMedian()
    res.to_csv("trip_miles_median.csv", index=False)
        

