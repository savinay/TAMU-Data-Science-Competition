import statistics
import pandas as pd

df = pd.read_csv("2016_Trip Total_sums.csv")
weeks = range(1,54)
median = {}
median["TripTotals"] = []
for week in weeks:
    data = df['week'+str(week)]
    values = [x for x in data.values if x != 0]
    # key = res_file.split("_")[1]
    
    
    if len(values) > 0:
        median["TripTotals"].append(statistics.median(values))
headers = ['Indicator', *[f'week{i}' for i in range(1, 54)]]
result = pd.DataFrame([[key, *val] for key, val in median.items()], columns=headers, index=None)
result.to_csv("2016_trip_totals_median.csv", index=False)

# df[df!=0].median()