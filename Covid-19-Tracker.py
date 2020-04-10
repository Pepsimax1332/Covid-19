from flask import Flask, render_template
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from flask_bootstrap import Bootstrap

from Graphs import Graphs
import pandas as pd

app = Flask(__name__)
Bootstrap(app)

graphs = Graphs()
nav = Nav()

nav.init_app(app)
print(pd.read_pickle("./data-frames/cleansed-data/UK.pkl").tail(1))


@nav.navigation()
def navbar():
    top_bar = Navbar('Covid-19 Tracker',
                     View("Home", 'home'),
                     View("UK", 'uk'))
    nav.register_element("top", top_bar)
    return top_bar

@app.route("/")
def home():

    pie = graphs.pi("", ["active_cases", "deaths", "total_recovered"], ["UK"])
    pie = graphs.get_json(pie)

    return render_template("home.html", pie=pie)


@app.route("/UK")
def uk():

    date = "2020-03-01"
    days = 7 + 4
    country_to_view = ["UK"]


    g = [["cases", "active_cases", "deaths", "total_recovered"],
         ["deaths/cases%", "recovered/cases%", "active/cases%"]]
    scatter = graphs.scatter("Cases %s" % country_to_view[0], g[0], country_to_view, y_label="Poeple Affected",
                             start_date=date)
    scatter = graphs.get_json(scatter)

    pie = graphs.pi("", ["active_cases", "deaths", "total_recovered"], ["UK"])
    pie = graphs.get_json(pie)


    return render_template("uk.html", pie=pie)


if __name__ == "__main__":

    app.run(debug=True)
