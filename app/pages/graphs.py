import dash
from dash import html
from dash import html, dcc, callback, dash_table, clientside_callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from dash_table import DataTable, FormatTemplate
from dash.exceptions import PreventUpdate
from dash.dash_table.Format import Format, Scheme, Trim
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
                #common.logger.info(str(data))
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
                #common.logger.info(str(stock_df[['date','p_name','available_to_sell']].head()))
                #stock_df['date'] = stock_df['date'].dt.date

                if keys: # if multiple columns then use index matching approach
                    idx_stock = stock_df.set_index(keys).index
                    idx_data = data_df.set_index(keys).index
                    dff = stock_df[idx_stock.isin(idx_data)]
                else: #if just p_name then filter on p_name
                    dff = stock_df[stock_df['p_name'].isin(data_df['p_name'].unique().tolist())]
                    keys = ['p_name']

                
                df_grouped = dff[['date'] + keys + ['available_to_sell']].groupby(keys + ['date']).agg({'available_to_sell':'sum'}).reset_index()
                df_grouped['Name'] = df_grouped[keys].apply(lambda x: ' - '.join(x.astype(str)),axis=1)
                
                df_graph = df_grouped.pivot(index='date',cols='Name',values='available_to_sell').reset_index()

                fig = px.line(df_graph,x='date',y=df_graph.columns),hover_data={'date':'%Y-%m-%d'}
                #common.logger.info(str(df_grouped[['date','available_to_sell']].head()))
                return html.Div([
                    dcc.Graph(id='graph_fig',figure = fig)
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

@callback(
    Output(''))