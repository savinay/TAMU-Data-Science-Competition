import addWeeks from addWeeks

def calculateSums(column, df):
    weeks = range(1,54)
    trip_miles={}
    for week in weeks:
        df_week = df[(df['week'] == week)]
        for _, val in df_week[['Taxi ID', column]].iterrows():
            if val[0] not in trip_miles:
                trip_miles[val[0]] = [0] * 53
            else:
                trip_miles[val[0]][week - 1] += val[1]

if __name__ == "__main__":
    df = pd.read_csv('Chicago_taxi_trips2017.csv', nrows = 1000000)
    df = addWeeks(df)
    calculateSums("Trip Miles", df)