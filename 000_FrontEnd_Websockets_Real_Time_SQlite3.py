
from dash import html, dcc, Output, Input, Dash, State
import sqlite3

update_frequency = 100

app = Dash()

app.layout = html.Div([
    html.H1(id="price-ticker"),
    dcc.Interval(id="update", interval = update_frequency)
])

@app.callback(
    Output("price-ticker", "children"),
    Input("update", "n_intervals"),
)
def update_data(intervals):
    conn = sqlite3.connect("./data.db")
    cursor = conn.cursor()

    data = cursor.execute("SELECT * FROM trades ORDER BY time DESC LIMIT 10").fetchall()

    #print(data)

    return data[0][3]


if __name__ == "__main__":
    app.run_server(debug=True)