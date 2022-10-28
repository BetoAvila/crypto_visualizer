from dash import Dash, Output, Input
from plotly import graph_objects as go
from setup import update_titles_, update_OHLC_chart_, update_VT_monitor_, update_indicators_
from PIL import Image
import dash_bootstrap_components as dbc
import json
import logging
import os


with open('settings.json', 'r') as f:
    configs = json.load(f)
logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO, datefmt='%Y-%b-%d %H:%M:%S')
os.chdir(configs['os']['pwd'])
assets = configs['os']['assets']

app = Dash(__name__, external_stylesheets=[dbc.themes.LUX])

@app.callback(
    Output('OHLC_title','children'),
    Output('vol_title','children'),
    Output('close_price_title','children'),
    Input('dropdown_crypto','value'),
    Input('radio_scope','value'),
    Input('refresher', 'n_intervals'))
def update_titles(crypto: str, scope: str, n: int) -> str:
    titles = update_titles_(crypto, scope)
    return titles[0], titles[1], titles[2]


@app.callback(
    Output('OHLC_chart','figure'),
    Input('dropdown_crypto','value'),
    Input('radio_scope','value'),
    Input('refresher', 'n_intervals'))
def update_OHLC_chart(crypto: str, scope: str, n: int) -> go.Figure:
    return update_OHLC_chart_(crypto, scope)


@app.callback(
    Output('volume_chart','figure'),
    Input('radio_scope','value'),
    Input('refresher', 'n_intervals'))
def update_VT_monitor(scope: str, n: int) -> go.Figure:
    return update_VT_monitor_(scope)


@app.callback(
    Output('indicator_BTC','figure'),
    Output('indicator_ETH','figure'),
    Output('indicator_LTC','figure'),
    Output('indicator_SOL','figure'),
    Output('indicator_BCH','figure'),
    Input('dropdown_crypto','value'),
    Input('radio_scope','value'),
    Input('refresher', 'n_intervals'))
def update_indicators(crypto: str, scope: str, n: int) -> tuple:
    figs = update_indicators_(crypto, scope)
    return figs[0], figs[1], figs[2], figs[3], figs[4]

@app.callback(
    Output('icon','src'),
    Input('dropdown_crypto','value'))
def update_icons(crypto: str) -> Image:
    return Image.open(f'{assets}{crypto.split("/")[0]}.png')