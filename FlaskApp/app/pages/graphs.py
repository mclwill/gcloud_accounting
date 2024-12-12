from datetime import datetime, date, time, timedelta
import dash
from dash import html
from dash import html, dcc, callback, dash_table, clientside_callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from dash import dash_table
from dash.dash_table import DataTable, FormatTemplate
from dash.exceptions import PreventUpdate
from dash.dash_table.Format import Format, Scheme, Trim
import plotly.express as px
import traceback
import pandas as pd
import json
import urllib.parse

import FlaskApp.app.common as common
import FlaskApp.app.data_store as data_store

dash.register_page(__name__)

layout = html.Div([
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
                dbc.Col([
                    html.Div([
                        html.Div(id='dd-output-container-graph',children='Data Update Complete')
                    ],style={'backgroundColor':'red','color':'white'})
                ]),
            ], align='center'),
            dbc.Row([
                dbc.Col(
                    html.Div([
                        dcc.Graph(id='graph-px-alt')
                    ])
                )
            ]),
            dbc.Row([
                dbc.Col(
                    dcc.RadioItems(
                        ['Absolute','Normalised'],
                         'Absolute',
                         id ='graph-type-alt',
                         inline=True,
                         inputStyle={"margin-right": "10px"}
                    )
                ,width={'size':1})
            ],justify='center')
        ],fluid=True),
    #dcc.Store(id = 'clientside-figure-store-px'),
    dcc.Location(id='url'),
    dcc.Store(id='df-store'),
    dcc.Store(id='name-text')
])

@callback(
    Output('df-store', 'data'),
    Output('name-text','data'),
    Input('url', 'search'),
    prevent_initial_call = True
)
def get_query(url):
    
    try:
        common.logger.debug('get query call back reached' + str(url))
        if url[1:].startswith('date'):  #dcc.Location 'search' return query string including '?' - so skip over that
            parsed_url = urllib.parse.urlparse(url)

            try:
                date = urllib.parse.parse_qs(parsed_url.query)['date'][0]
                data = urllib.parse.parse_qs(parsed_url.query)['data'][0]
            except Exception as ex: #(ValueError, KeyError):
                common.logger.info('graph query with invalid parameters - url: ' + str(url) + '\n' + str(ex))
                return None,None
            #data = url[1:].replace('data=','')
            #common.logger.info('URL call back reached' + str(data))
            data = json.loads(urllib.parse.unquote(data)) #need to decode url string before sending through json decoder

            common.logger.debug(str(data))
            if data:
                data_df = pd.DataFrame.from_records(data)
                data_cols = data_df.columns.tolist()
                if 'color' in data_cols:
                    if 'size' in data_cols:
                        keys = ['color','size']
                        name_text = 'Colour - Size'
                    else:
                        keys = ['color']
                        name_text = 'Colour'
                elif 'size' in data_cols:
                    keys = ['size']
                    name_text = 'Size'
                else:
                    keys = []
                    name_text = ''
                if 'p_name' in data_cols:
                    keys.insert(0,'p_name')
                    if name_text : 
                        name_text = 'Product - ' + name_text 
                    else:
                        name_text = 'Product'
                if 'sub_category' in data_cols:
                    keys.insert(0,'sub_category')
                    if name_text:
                        name_text = 'Sub-Category - ' + name_text
                    else:
                        name_text = 'Sub-Catgegory'

                common.logger.debug(str(type(date)) + '\n' + str(date))
                stock_df = data_store.get_data_from_globals()[0].copy()  
                stock_df = stock_df[stock_df['date'] >= datetime.strptime(date,'%Y-%m-%d').date()]
                common.logger.debug(str(stock_df))
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

                '''fig = px.line(df_graph,x='date',y=plot_cols,title='Available To Sell History',\
                                       labels={'variable':name_text,\
                                               'date':'Date',\
                                               'value':'Stock Available to Sell'},
                                        #mode = 'markers+lines'
                                    )
                fig.update_layout(
                    height = 600
                )
                '''
                return df_graph.to_json(date_format='iso', orient='split'), name_text

        return None,None

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))
        return None,None

@callback(
    Output('graph-px-alt', 'figure'),
    [Input('df-store','data'),
     Input('graph-type-alt', 'value'),
     Input('name-text','data')],
     running=[(Output("dd-output-container-graph","children"),'Data Being Updated.....Please Wait', 'Data Update Complete'),
                 (Output("dd-output-container-graph","style"),{'backgroundColor':'red','color':'white'},{'backgroundColor':'white','color':'black'})],
     prevent_initial_call = True
)
def update_figure(df_graph,graph_type,name_text):
    
    try:
        common.logger.debug('update figure call back reached' + str(graph_type) + str(name_text) + str(df_graph))
        if df_graph:
            df_graph = pd.read_json(df_graph, orient='split')
            if graph_type == 'Absolute':
                plot_cols = [x for x in df_graph.columns.tolist() if '_norm' not in x]
                dff = df_graph[plot_cols]
            else:
                plot_cols = [x for x in df_graph.columns.tolist() if '_norm' in x]
                dff = df_graph[plot_cols]

            fig = px.line(df_graph,x='date',y=plot_cols,title='Available To Sell History - ' + graph_type,hover_data={'date':"|%Y-%m-%d"},markers=True,\
                                               labels={'variable':name_text,\
                                                       'date':'Date',\
                                                       'value':'Stock Available to Sell'}\
                            )
            fig.update_layout(
                        height = 600,
                    )
            fig.update_yaxes(rangemode='tozero')
            return fig 

        return {}

    except Exception as ex:
        tb = traceback.format_exc()
        common.logger.warning('Error Process Dashboard Layout' + '\nException Info: ' + str(ex) + '\nTraceback Info: ' + str(tb))
        return {}


