# pylint: skip-file
import pandas as pd
from collections import Counter
if __name__ == "__main__":
    counts = {}
    # for i in range(2013, 2018):
    i = 2013
    filename = f"Chicago_taxi_trips{i}.csv"
    df = pd.read_csv(filename,
                        usecols=["Taxi ID", "Trip Start Timestamp"],
                        dtype={"Taxi ID": object, "Trip Start Timestamp": object}
                        )
    addWeeks(df).groupby("week")["Taxi ID"].nunique()
