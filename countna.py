for year in range(2013, 2018):
    filename = f"{folder}/Chicago_taxi_trips{year}.csv"
    df = pd.read_csv(filename,
                        usecols=DATATYPES.keys(),
                        dtype=DATATYPES)

df.isnull().sum()