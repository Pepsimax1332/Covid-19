import plotly as pl
import pandas as pd
from datetime import datetime, timedelta
from scipy import optimize
import json


class Graphs:

    def __init__(self):
        pd.set_option('display.max_rows', 300)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)
        self.date = datetime.today().strftime("%Y-%m-%d")

        self.path = "./data-frames/countries-affected/countries-affected_%s.pkl" % self.date
        self.countries = list(pd.read_pickle(self.path)["affected_countries"])

        self.path = "./data-frames/cleansed-data/"

        self.functions = {"exp": lambda x, e, b: x ** e + b,
                          "exp_zero": lambda x, e: x ** e,
                          "sigmoid": lambda x, e, b: (1 / (1 + e ** (-x))) + b,
                          "sigmoid_zero_y": lambda x, e: (1 / (1 + e ** (-x)))}

    def get_dates(self, period, start):
        dates = pd.date_range(start=start, periods=period)
        return [str(date).split(' ')[0] for date in dates]

    def predict(self, label, country, function, start_date, days):

        df = pd.read_pickle("./data-frames/cleansed-data/%s.pkl" % country)
        df["statistic_taken_at"] = pd.to_datetime(df["statistic_taken_at"])
        mask = (df["statistic_taken_at"] >= datetime.strptime(start_date, "%Y-%m-%d"))
        df = df.loc[mask]
        df_to_return = pd.DataFrame()

        df = df.loc[df[label] > 0]

        x = list(range(len(df[label])))

        popt, popc = optimize.curve_fit(function, x, df[label])
        df_to_return[label] = [function(x, *popt) for x in list(range(len(df[label]) + days))]

        df_to_return["statistic_taken_at"] = self.get_dates(len(df_to_return[label]), start_date)

        return [df_to_return, popt, popc]

    def get_data_between_dates(self, data, start_date=None, end_date=None):

        data["statistic_taken_at"] = pd.to_datetime(data["statistic_taken_at"])

        if start_date == end_date:
            end_date = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)
            end_date = str(end_date).split(" ")[0]

        if start_date is not None and end_date is not None:
            mask = (data["statistic_taken_at"] >= datetime.strptime(start_date, "%Y-%m-%d"))
            data = data.loc[mask]
            mask = (data["statistic_taken_at"] <= datetime.strptime(end_date, "%Y-%m-%d"))
            data = data.loc[mask]

        elif start_date is not None:
            mask = (data["statistic_taken_at"] >= datetime.strptime(start_date, "%Y-%m-%d"))
            data = data.loc[mask]

        elif end_date is not None:
            mask = (data["statistic_taken_at"] <= datetime.strptime(end_date, "%Y-%m-%d"))
            data = data.loc[mask]
        return data

    def format_dates(self, data):

        dates = pd.to_datetime(data["statistic_taken_at"])
        dates = dates.apply(lambda x: x.strftime('%Y-%m-%d'))
        dates = dates.reset_index(drop=True)

        return dates

    def get_figure(self, fig):

        if fig is not None:
            fig = fig
        else:
            fig = pl.graph_objs.Figure()

        return fig

    def get_data(self, df=None, country=None):

        if df is not None:
            data = df
        else:
            data = pd.read_pickle(self.path + country + ".pkl")

        return data

    def scatter(self, title, labels, countries, x_label=None, y_label=None, width=None, height=None, start_date=None,
                end_date=None, fig=None, df=None):

        fig = self.get_figure(fig)

        for country in countries:

            data = self.get_data(df, country)
            data = self.get_data_between_dates(data, start_date, end_date)
            dates = self.format_dates(data)

            for label in labels:
                fig.add_trace(pl.graph_objs.Scatter(
                    x=dates,
                    y=data[label],
                    mode='markers+lines',
                    name=label+" "+country,
                ))

        fig.update_layout(
            width=width,
            height=height,
            xaxis_title={'text': x_label},
            yaxis_title={'text': y_label, 'font': {'size': 16}},
            margin=dict(l=0, r=0, b=0, t=25, pad=0))
        return fig

    def bar(self, title, labels, countries, x_label=None, y_label=None, width=None, height=None, start_date=None,
            end_date=None, fig=None, df=None):

        fig = self.get_figure(fig)

        for country in countries:

            data = self.get_data(df, country)
            data = self.get_data_between_dates(data, start_date)
            dates = self.format_dates(data)

            for label in labels:
                fig.add_trace(pl.graph_objs.Bar(
                    x=dates,
                    y=data[label],
                    name=label + "-" + country))

        fig.update_layout(title={'text': title,
                                 'y': 0.95,
                                 'x': 0.45,
                                 'xanchor': 'center',
                                 'yanchor': 'top',
                                 'font': {'size': 36}},
                          width=width,
                          height=height,
                          barmode="group",
                          xaxis_title={'text': x_label, 'font': {'size': 24}},
                          yaxis_title={'text': y_label, 'font': {'size': 24}},
                          font=dict(size=16))
        return fig

    def pi(self, title, labels, countries, x_label=None, y_label=None, width=None, height=None, date=None,
           fig=None, df=None):
        values = []
        fig = self.get_figure(fig)
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
            print(date)

        if len(countries) > 1 and len(labels) == 1:
            print(True)
            label = labels[0]
            for country in countries:
                data = self.get_data(df, country)
                data = self.get_data_between_dates(data, date, date)
                print(data[label])
                values.append(float(data[label]))

                fig.add_trace(pl.graph_objs.Pie(title={'text': label.capitalize(),
                                                       'font': {'size': 36}},
                                                labels=countries,
                                                values=values,
                                                hole=0.3))

        elif len(labels) > 1 and len(countries) == 1:
            country = countries[0]

            data = self.get_data(df, country)
            data = self.get_data_between_dates(data, date, date)

            for label in labels:
                values.append(float(data[label]))

            fig.add_trace(pl.graph_objs.Pie(title={'text': title + " " + country,
                                                   'font': {'size': 36}},
                                            labels=labels,
                                            values=values,
                                            hole=0.3))

        return fig

    def get_json(self, fig):
        return json.dumps(fig, cls=pl.utils.PlotlyJSONEncoder)


if __name__ == "__main__":
    graphs = Graphs()

    countries_ = pd.read_pickle("./data-frames/countries-affected/countries-affected_2020-03-21.pkl")[
        "affected_countries"].head(20)

    date = "2020-03-01"
    days = 7 + 4
    country_to_view = ["USA"]

    h = [["cases", "active_cases", "deaths", "total_recovered"],
         ["deaths/cases%", "recovered/cases%", "active/cases%"]]

    for g in h:
        scatter = graphs.scatter("Cases", g, countries_, y_label="Poeple Affected",
                                 start_date=date)
        scatter.show()
        #
        # bar = graphs.bar("Cases %s" % country_to_view, g, country_to_view, y_label="Poeple Affected", start_date=date)
        # bar.show()
        #
        # pi = graphs.pi("Cases", g[1:], country_to_view, y_label="Poeple Affected", date=None)
        # pi.show()
        #
        # pi = graphs.pi("", [g[0]], countries_, y_label="Poeple Affected", date=None)
        # pi.show()
