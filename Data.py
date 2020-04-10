import http.client
import ast
import pandas as pd
import os.path
from datetime import datetime

"""
This class is designed to pull data from https://rapidapi.com/astsiatsko/api/coronavirus-monitor.
To use this you will need to sign up and get a key from them.

Country history is updated by them every ten mins. As such you can opt to get all world history
by setting getWorldHistoryData to True. If this is not set this class will only pull detailed history
from the UK by default. 

All other data that is less intensive to pull is updated. Additionally, cleansed data was spliced with 
data from https://data.europa.eu/euodp/en/data/dataset/covid-19-coronavirus-data to give a history 
back to the start of the year for most countries. I cannot myself say how accurate this was but for
the most part it seemed to check out.

I will be adding functionality to scrape data from 
https://www.arcgis.com/apps/opsdashboard/index.html#/f94c3c90da5b4e9f9a0b19484dd4bb14 so that regional
UK data will be pulled and updated as quickly as the they do.
"""


class Data:
    pd.set_option('display.max_rows', 300)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    def __init__(self, key=None, getWorldHistoryData=False):
        self.conn = http.client.HTTPSConnection("coronavirus-monitor.p.rapidapi.com")

        self.headers = {
            'x-rapidapi-host': "coronavirus-monitor.p.rapidapi.com",
            'x-rapidapi-key': key
        }

        self.date = datetime.today().strftime("%Y-%m-%d")
        self.affected_countries = None

        self.update_affected_countries()

        if getWorldHistoryData:
            self.update_history_by_affected_country()
        else:
            self.update_country_history()

        self.update_cases_by_country()
        self.update_world_stats()
        self.update_cleansed_data()

    def update_cleansed_data(self):
        # print(self.affected_countries)

        for country in self.affected_countries["affected_countries"]:

            path = "./data-frames/cleansed-data/%s.pkl" % country
            today = str(datetime.today()).split(" ")[0]
            try:
                df = pd.read_pickle(path)

                last = df.tail(1)
                if today in str(last["statistic_taken_at"]):
                    df = df.head(len(df['cases']) - 1)
                    print("Updating 'cleansed data' data-frame %s" % country)

                else:
                    print("Adding to 'cleansed data' data frame %s" % country)

            except Exception as e:
                print(e)

                df = pd.DataFrame()

            to_append = pd.read_pickle("./data-frames/cases-by-country/cases-by-country_%s.pkl" % today)
            to_append = to_append.loc[to_append['country_name'] == country]

            df = df.append(to_append)
            df["active/cases%"] = round(df["active_cases"] / df["cases"] * 100, 2)
            df["deaths/cases%"] = round(df["deaths"] / df["cases"] * 100, 2)
            df["recovered/cases%"] = round(df["total_recovered"] / df["cases"] * 100, 2)
            df = df.drop(["country_name", "region"], axis=1)

            df = df.reset_index(drop=True)

            df.to_pickle(path)

    def update_affected_countries(self):

        self.affected_countries = self.get_affected_countries()

        path = "./data-frames/countries-affected/countries-affected_%s.pkl" % self.date
        df = self.get_affected_countries()

        if os.path.isfile(path):
            print("Updating 'affected countries' data frame %s" % self.date)
            df.to_pickle(path)
        else:
            print("Creating 'affected countries' data frame %s" % self.date)
            df.to_pickle(path)

    def update_history_by_affected_country(self):

        for country in self.affected_countries["affected_countries"]:
            path = "./data-frames/countries-affected-history/%s.pkl" % country
            df = self.get_history_by_affected_country(country)

            if os.path.isfile(path):
                print("Updating 'history by affected country' data frame %s" % country)
                df.to_pickle(path)
            else:
                df.to_pickle(path)
                print("Creating 'history by affected country' data frame %s" % country)

    def update_country_history(self, country="UK"):

        path = "./data-frames/countries-affected-history/%s.pkl" % country
        df = self.get_history_by_affected_country("UK")

        if os.path.isfile(path):
            print("Updating 'history by affected country' data frame %s" % country)
            df.to_pickle(path)
        else:
            df.to_pickle(path)
            print("Creating 'history by affected country' data frame %s" % country)

    def update_cases_by_country(self):

        path = "./data-frames/cases-by-country/cases-by-country_%s.pkl" % self.date
        df = self.get_cases_by_country()

        if os.path.isfile(path):
            print("Updating 'cases by country' data frame  %s" % self.date)
            df.to_pickle(path)
        else:
            df.to_pickle(path)
            print("Creating 'cases by country' data frame %s" % self.date)

    def update_world_stats(self):

        path = "./data-frames/global-data/world-stats_%s.pkl" % self.date
        df = self.get_world_total_stats()

        if os.path.isfile(path):
            print("Updating 'world stats' data frame %s" % self.date)
            df.to_pickle(path)
        else:
            df.to_pickle(path)
            print("Creating 'world stats' data frame %s" % self.date)

    def get_affected_countries(self):
        self.conn.request("GET", "/coronavirus/affected.php", headers=self.headers)

        res = self.conn.getresponse()
        data = res.read()

        df = pd.DataFrame(ast.literal_eval(data.decode("utf-8")))

        return df

    def get_history_by_affected_country(self, country):
        if "ç" in country or "é" in country:
            url = "/coronavirus/cases_by_particular_country.php?country=R%25C3%25A9union"
            return pd.DataFrame({country: ["ENCODEING ERROR"]})  # ENCODING BUG BYPASSING FOR NOW
        else:
            url = "/coronavirus/cases_by_particular_country.php?country=%s" % country.replace(" ", "%20")

        self.conn.request("GET", url, headers=self.headers)

        res = self.conn.getresponse()
        data = res.read().decode("utf-8")
        data = data.replace('{"country":"%s","stat_by_country":[' % country, "").replace("]", "").replace("null",
                                                                                                          '"null"').split(
            "},")
        df = pd.DataFrame()

        for i in range(len(data)):
            if country in data[i]:
                if i != len(data) - 1:
                    df = df.append(ast.literal_eval(data[i] + "}"), ignore_index=True)
                else:
                    df = df.append(ast.literal_eval(data[i][:-1]), ignore_index=True)

        df["active_cases"] = self.steralize(df["active_cases"], int)
        df["id"] = self.steralize(df["id"], int)
        df["new_cases"] = self.steralize(df["new_cases"], int)
        df["new_deaths"] = self.steralize(df["new_deaths"], int)
        df["serious_critical"] = self.steralize(df["serious_critical"], int)
        df["total_cases"] = self.steralize(df["total_cases"], int)
        df["total_cases_per1m"] = self.steralize(df["total_cases_per1m"], float)
        df["total_deaths"] = self.steralize(df["total_deaths"], int)
        df["total_recovered"] = self.steralize(df["total_recovered"], int)

        df["total deaths/cases%"] = df["total_deaths"] / df["total_cases"] * 100
        df["total recovered/cases%"] = df["total_recovered"] / df["total_cases"] * 100
        df = df.round({"total deaths/cases%": 2, "total recovered/cases%": 2})

        return df

    def get_cases_by_country(self):
        self.conn.request("GET", "/coronavirus/cases_by_country.php", headers=self.headers)

        res = self.conn.getresponse()
        data = res.read().decode("utf-8")

        data = ast.literal_eval(data)

        df = pd.DataFrame(data["countries_stat"])

        print(df)

        df["cases"] = self.steralize(df["cases"], int)
        df["deaths"] = self.steralize(df["deaths"], int)
        df["total_recovered"] = self.steralize(df["total_recovered"], int)
        df["new_cases"] = self.steralize(df["new_cases"], int)
        df["serious_critical"] = self.steralize(df["serious_critical"], int)
        df["active_cases"] = self.steralize(df["active_cases"], int)
        df["total_cases_per_1m_population"] = self.steralize(df["total_cases_per_1m_population"], float)

        df["deaths/cases%"] = df["deaths"] / df["cases"] * 100
        df["recovered/cases%"] = df["total_recovered"] / df["cases"] * 100
        df["active/cases%"] = df["active_cases"] / df["cases"] * 100

        df = df.round({"deaths/cases%": 2, "recovered/cases%": 2, "active/cases%": 2})

        df["statistic_taken_at"] = data["statistic_taken_at"]

        return df

    def get_world_total_stats(self):
        self.conn.request("GET", "/coronavirus/worldstat.php", headers=self.headers)

        res = self.conn.getresponse()
        data = res.read().decode("utf-8")
        data = ast.literal_eval(data)

        data_to_save = {}

        for key, val in data.items():
            data_to_save[key] = []

        for key, val in data.items():
            data_to_save[key].append(val)

        df = pd.DataFrame(data_to_save)

        df['total_cases'] = self.steralize(df['total_cases'], int)
        df['total_deaths'] = self.steralize(df['total_deaths'], int)
        df['total_recovered'] = self.steralize(df['total_recovered'], int)
        df['new_cases'] = self.steralize(df['new_cases'], int)
        df['new_deaths'] = self.steralize(df['new_deaths'], int)

        df["deaths/cases%"] = df["total_deaths"] / df["total_cases"] * 100
        df["recovered/cases%"] = df["total_recovered"] / df["total_cases"] * 100
        df = df.round({"deaths/cases%": 2, "recovered/cases%": 2})

        return df

    def get_pre_api_data(self):

        df = pd.read_csv("./data-frames/COVID-19-geographic-disbtribution-worldwide-2020-03-18.csv")

        for country in df["Countries and territories"].drop_duplicates():
            data = df[df["Countries and territories"] == country.replace(" ", "_")]
            data["DateRep"] = pd.to_datetime(data["DateRep"])  # - np.timedelta64(1, 'D')
            data = data.iloc[::-1]

            data["total_cases"] = data["Cases"].cumsum()
            data["total_death"] = data["Deaths"].cumsum()

            print("Creating 'pre-api' data set %s " % country)
            data.to_pickle("./data-frames/pre-api/%s.pkl" % country)

    def determine_irregularities(self):
        df = pd.read_csv("./data-frames/COVID-19-geographic-disbtribution-worldwide-2020-03-18.csv")

        df2 = pd.read_pickle("./data-frames/countries-affected/countries-affected_2020-03-19.pkl")
        i = 0
        for country in df["Countries and territories"].drop_duplicates():
            if country not in list(df2["affected_countries"]):
                print(i, country)
            i += 1

    def steralize(self, column, type):

        return pd.to_numeric(column.str.replace(",", "").astype(type, errors='ignore'), errors="coerce")


def run(key=None, getWorldHistoryData=False):
    import time
    start = time.time()
    data = Data(key=key, getWorldHistoryData=getWorldHistoryData)
    end = round(time.time() - start, 3)
    message = "Updated data in %.2f seconds on %s" % (end, datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    print(message)
    with open("./data-frames/updates-log.txt", "a+") as file:
        file.write(message + '\n')

    return data


def cl():
    df = pd.read_pickle("./data-frames/UK.pkl")
    df = df.reset_index(drop=True)
    df["statistic_taken_at"] = pd.to_datetime(df["statistic_taken_at"])
    dx = df.tail(81 - 63)

    df = pd.read_csv("./data-frames/COVID-19-geographic-disbtribution-worldwide-2020-03-18 .csv")

    df = df.drop(["Day", "Month", "Year", "GeoId"], axis=1)
    df = df.rename(columns={"Cases": "new_cases", "Deaths": "new_deaths", "DateRep": "statistic_taken_at"})

    dates = ["2020-03-18", "2020-03-19", "2020-03-20"]
    for country in df["Countries and territories"].drop_duplicates():
        data = df.loc[df["Countries and territories"] == country]
        data = data.iloc[::-1]
        data["cases"] = data["new_cases"].cumsum()
        data["deaths"] = data["new_deaths"].cumsum()

        data["statistic_taken_at"] = pd.to_datetime(data["statistic_taken_at"]) - pd.offsets.Day(1)
        data = data.drop(["Countries and territories"], axis=1)

        for date in dates:
            to_append = pd.read_pickle("./data-frames/cases-by-country/cases-by-country_%s.pkl" % date)
            data = data.append(to_append.loc[to_append["country_name"] == country])

        data = data.drop(["country_name", "region"], axis=1)

        if country == "UK":
            data = data.head(63).append(dx.tail(81 - 63))
            data = data.append(to_append.loc[to_append["country_name"] == country])

        data = data.reset_index(drop=True)
        data["active/cases%"] = round(data["active_cases"] / data["cases"], 2)
        data["deaths/cases%"] = round(data["deaths"] / data["cases"], 2)

        data.to_pickle("./data-frames/cleansed-data/%s.pkl" % country)


if __name__ == "__main__":
    data = run(key="09c05c32f8msh142adc1360507a5p1eb1d9jsn26c3269eb8b2", getWorldHistoryData=False)

    print(pd.read_pickle("./data-frames/cleansed-data/UK.pkl").tail(20))


