import dash
from dash import html
import plotly.express as px
import traceback
import pandas as pd
import json

import FlaskApp.app.common as common
from FlaskApp.app.data_store import get_data_from_globals

dash.register_page(__name__)

def layout(**kwargs):
    
    try:
        data = kwargs.pop('data',None)

        if data:
            data = json.loads(data)
            if data:
                common.logger.info(str(data))
                data_df = pd.DataFrame.from_records(data)
                data_cols = data_df.columns.tolist()
                if 'color' in data_cols:
                    if 'size' in data_cols:
                        keys = ['p_name','color','size']
                    else:
                        keys = ['p_name','color']
                elif 'size' in data_cols:
                    keys = ['p_name','size']
                else:
                    keys = False

                stock_df = get_data_from_globals()[0].copy()

                if keys: # if multiple columns then use index matching approach
                    idx_stock = stock_df.set_index(keys).index
                    idx_data = data_df.set_index(keys).index
                    dff = stock_df[idx_stock.isin(idx_data)]
                else: #if just p_name then filter on p_name
                    dff = stock_df[stock_df['p_name'].isin(data_df['p_name'].unique().tolist())]

                return html.Div([
                    dcc.Graph(id='graph_fig',fig = px.line(dff,x='date',y='available_to_sell'))
                ])
        return html.Div([
            html.Div('No data to display')
        ])

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Graphing Info' + '\nException Info: ' + str(ex) + '/nTraceback Info: ' + str(tb))
        return html.Div(
                html.P('Error processing graph')
        ) 