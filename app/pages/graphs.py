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

graphs_layout = html.Div([
                    dbc.Container(
                        children= [
                            dbc.Row([
                                dbc.Col(
                                    dbc.Card([
                                        dbc.CardBody([
                                            html.H1("Dashboard"),
                                            html.P('''
                                                 This is a dashboard for A.Emery
                                                 '''),
                                        ]),   
                                    ],className="border-0 bg-transparent"),
                                    width={"size":3} 
                                ),
                                dbc.Col(
                                    dbc.Card([
                                        dbc.CardBody([
                                            html.Div([
                                                dbc.Button("LOGOUT",href='/logout',color='light',size='lg',external_link=True,)
                                            ]),
                                        ]),
                                    ],className="border-0 bg-transparent"),
                                    width={"size":1,'offset':8}
                                )
                            ]),
                            dbc.Row([
                                dbc.Col(
                                    html.Div([
                                        dcc.Graph(id='graph-px',figure = fig)
                                    ])
                                )
                            ]),
                            dbc.Row([
                                dbc.Col(
                                    dcc.RadioItems(
                                        ['Absolute','Normalised'],
                                         'Absolute',
                                         id ='graph-type'
                                    )
                                )
                            ])
                        ],fluid=True),
                    #dcc.Store(id = 'clientside-figure-store-px'),
                ])

def layout(**kwargs):
    global df_graph

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
                        name_text = 'Product - Colour - Size'
                    else:
                        keys = ['p_name','color']
                        name_text = 'Product - Colour'
                elif 'size' in data_cols:
                    keys = ['p_name','size']
                    name_text = 'Product - Size'
                else:
                    keys = False
                    name_text = 'Product'

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

                df_graph = df_grouped.pivot(index='date',columns='Name',values='available_to_sell')

                for col in df_graph.columns.tolist():
                    col_max = df_graph[col].max()
                    col_min = df_graph[col].min()
                    abs_max = max(col_max,-col_min)
                    df_graph[col + '_norm'] = df_graph[col] / col_max * 100

                plot_cols = [x for x in df_graph.columns.tolist() if '_norm' not in x] #get normalised columns before bringing back 'date'
                
                df_graph = df_graph.reset_index()  #bring back 'date' into columns

                #common.logger.info(str(df_graph.head()) + '\n' + str(plot_cols))

                fig = px.line(df_graph,x='date',y=plot_cols,hover_data={'date':'%Y-%m-%d'},title='Available To Sell History',\
                                       labels={'variable':name_text,\
                                               'date':'Date',\
                                               'value':'Stock Available to Sell'}
                                    )
                fig.update_layout(
                    height = 600
                )
                #common.logger.info(str(df_grouped[['date','available_to_sell']].head()))
                return graphs_layout


        #return error if fall through to here

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
    Output('graph-px', 'figure'),
    Input('graph-type', 'value')
)
def update_store_data(type):
    global df_graph
    if type == 'Absolute':
        plot_cols = [x for x in df_graph.columns.tolist() if '_norm' not in x]
        dff = df_graph[plot_columns]
    else:
        plot_cols = [x for x in df_graph.columns.tolist() if '_norm' in x]
        dff = df_graph[plot_columns]
    return px.line(df_graph,x='date',y=plot_cols,hover_data={'date':'%Y-%m-%d'},title='Available To Sell History',\
                                       labels={'variable':name_text,\
                                               'date':'Date',\
                                               'value':'Stock Available to Sell'}\
                    )
'''
clientside_callback(
    """
    function(data) {
        return {
            'data': data,
             }
        }
    }
    """,
    Output('clientside-graph-px', 'figure'),
    Input('clientside-figure-store-px', 'data')
)'''
