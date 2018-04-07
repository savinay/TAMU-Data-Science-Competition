# pylint: skip-file
import pandas as pd


def generateData():
    TaxiID1 = "0013da5489fe976daf4f4a7d246073ecf2caed9b12b3be70eff37b63ef9ca4102c972145ddd8537811752d51b222b4618dfbae451b966d7a3ad9afa4ec878a6e"
    data = {
        "Taxi ID": [TaxiID1, TaxiID1, TaxiID1, TaxiID1, TaxiID1, TaxiID1],
        "Trip Start Timestamp": ["01/01/2015", "01/08/2015", "01/15/2015", "01/01/2015", "01/08/2015", "01/15/2015"],
        "Trip Miles": [1, 2, 3, 1, 2, 3]
    }
    return pd.DataFrame(data=data)


if __name__ == "__main__":
    result = generateData()
    result.to_csv("testdata.csv", index=False)
