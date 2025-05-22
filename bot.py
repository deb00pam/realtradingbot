import os
import numpy as np
import pandas as pd
from flask import Flask, render_template, request
import plotly.graph_objs as go
from scipy.signal import find_peaks
import plotly.io as pio

app = Flask(__name__)
pio.renderers.default = 'browser'

def kalman_filter(series, process_var=1e-5, measurement_var=0.01**2):
    n = len(series)
    xhat = np.zeros(n)
    P = np.zeros(n)
    xhatminus = np.zeros(n)
    Pminus = np.zeros(n)
    K = np.zeros(n)

    xhat[0] = series.iloc[0]
    P[0] = 1.0

    for k in range(1, n):
        xhatminus[k] = xhat[k-1]
        Pminus[k] = P[k-1] + process_var
        K[k] = Pminus[k] / (Pminus[k] + measurement_var)
        xhat[k] = xhatminus[k] + K[k] * (series.iloc[k] - xhatminus[k])
        P[k] = (1 - K[k]) * Pminus[k]

    return xhat

def load_and_analyze(symbol):
    filepath = f'data/{symbol}.parquet'
    if not os.path.exists(filepath):
        return None, f"No data for symbol {symbol}"

    df = pd.read_parquet(filepath)
    df['smooth'] = kalman_filter(df['close'])
    peaks, _ = find_peaks(df['smooth'], distance=10, prominence=0.1)
    troughs, _ = find_peaks(-df['smooth'], distance=10, prominence=0.1)

    fig = go.Figure(data=[
        go.Candlestick(x=df['time'], open=df['open'], high=df['high'],
                       low=df['low'], close=df['close'],
                       increasing_line_color='green', decreasing_line_color='red')
    ])
    fig.add_trace(go.Scatter(x=df.time, y=df['smooth'], name='Kalman Smooth', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=df.time[peaks], y=df['smooth'].iloc[peaks],
                             mode='markers', marker=dict(color='red', size=10), name='Peaks'))
    fig.add_trace(go.Scatter(x=df.time[troughs], y=df['smooth'].iloc[troughs],
                             mode='markers', marker=dict(color='green', size=10), name='Troughs'))

    fig.update_layout(height=600, margin=dict(t=40, b=40, l=20, r=20))
    graph_html = fig.to_html(full_html=False)

    return graph_html, None

@app.route('/', methods=['GET', 'POST'])
def index():
    symbols = [f.replace('.parquet', '') for f in os.listdir('data') if f.endswith('.parquet')]
    selected_symbol = request.form.get('symbol', symbols[0] if symbols else None)
    chart_html, error = load_and_analyze(selected_symbol) if selected_symbol else ("", "No symbol selected")
    return render_template('index.html', symbols=symbols, selected=selected_symbol,chart=chart_html, error=error)

if __name__ == '__main__':
    app.run(debug=True)
