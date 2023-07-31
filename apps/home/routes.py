# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
from apps.home import blueprint
from flask import render_template, request, session, jsonify
from flask_login import login_required
from jinja2 import TemplateNotFound
import plotly.graph_objs as go
from flask import render_template

import pymysql
import mysql.connector as mysql
import numpy as np
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy import text as sqlalctext  # edit st 2023-03-07
import pandas as pd
import scipy.signal

import matplotlib.dates as mdates
from datetime import datetime
import plotly.graph_objects as go

engine = create_engine(
    'mysql+pymysql://sandbox_read_only:ThX*AXrE%1W4X27@mysqldatabase.cmi5f1vp8ktf.us-east-1.rds.amazonaws.com:3306/sandbox')

# creating a connection object
connection = engine.connect()

# SQL query
stmt = "SELECT * FROM digital_demand WHERE (gt_category = 13) AND (country = 'DE') and (date >= '2022-11-01')"

# Execute the query and store the result in a DataFrame
df = pd.read_sql(sqlalctext(stmt), connection)
connection.close()

# df_raw = pd.read_excel('df_raw_data.xlsx')

df.date = pd.to_datetime(df['date'])

# text = session.get('text', 'vodafone')
texts = ["vodafone", "1und1", "o2", "telekom" ]
# text = "vodafone"
init_date = "2022-12-01"
index_date = "2022-12-01"
output_type = "png"


def add_ma(df, var, window):

    var_new = var + '_ma'  # new ma variable to be added to df
    df = df.sort_values(by=['keyword',
                            'gt_category',
                            'country',
                            'date'
                            ])
    df[var_new] = df.groupby(['keyword',
                              'country',
                              'gt_category'
                              ])[var].transform(lambda x: x.rolling(window).mean())  # compute moving average

    df = df.rename(columns={var_new: var_new+str(window)})
    return df


def add_smoother(df, var, cutoff):

    b, a = scipy.signal.butter(3, cutoff)
    var_new = var + '_smooth'  # new ma variable to be added to df
    df = df.sort_values(by=['keyword',
                            'gt_category',
                            'country',
                            'date'
                            ])
    df[var_new] = df.groupby(['keyword',
                              'country',
                              'gt_category'
                              ])[var].transform(lambda x: scipy.signal.filtfilt(b, a, x))  # compute moving average
    return df


def add_indexing(df, var, index_date):

    var_ref = var + '_ref'  # variable for index computation
    var_new = var + '_index'  # new index variable to be added to df
    # create reference df with values from indexdate
    df_ref = df[df['date'] == index_date]
    df_ref = df_ref.rename(columns={var: var_ref})  # rename to avoid confusion
    # Add values of indexdate to original dataframe and compute index values
    df_w_index = pd.merge(df, df_ref[['keyword',
                                      'country',
                                      'gt_category',
                                      var_ref]],
                          how="left",
                          on=['keyword',
                              'country',
                              'gt_category'
                              ])
    df_w_index[var_new] = (df_w_index[var]/df_w_index[var_ref])*100
    return df_w_index




def single(df_raw, key, geo, cat, startdate, index, indexdate, font_use, out_type):
    df_key = df_raw[(df_raw.keyword == key)
                    & (df_raw.country == geo)
                    & (df_raw.gt_category == cat)]
    if index == True:
        df_key = add_indexing(df_key, 'vl_value', indexdate)
        var_new = 'vl_value_index'
    else:
        var_new = 'vl_value'
        # running the functions we created to create moving average, smoother
    df_key = add_ma(df_key, var_new, 14)
    df_key = add_smoother(df_key, var_new, 0.02)

    df = df_key[df_key.date >= startdate]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df.date,
            y=df[var_new],
            name='original',
            mode='lines',
            opacity=0.3,
            line=dict(color='#024D83',
                      width=4),
            showlegend=True
        ))
    # creating the trendline values
    df_trend = df[['date', var_new]]  # i.e we need date and vl_value
    # dropping 0 because trendlines can't cope without numeric values
    df_trend0 = df_trend.dropna()
    x_sub = df_trend0.date
    y_sub = df_trend0[var_new]
    # transforming dates to numeric values, necessary for polynomial fitting
    x_sub_num = mdates.date2num(x_sub)
    z_sub = np.polyfit(x_sub_num, y_sub, 1)  # polynomial fitting
    p_sub = np.poly1d(z_sub)
    # adding the trendline trace
    fig.add_trace(
        go.Scatter(
            x=x_sub,
            y=p_sub(x_sub_num),
            name='trend',
            mode='lines',
            opacity=1,
            line=dict(color='green',
                      width=4,
                      dash='dash')
        ))
    # adding the 2 week's moving avg trace
    fig.add_trace(
        go.Scatter(
            x=df.date,
            y=df[var_new+'_ma'+str(14)],
            name=var_new+'_ma'+str(14),
            mode='lines',
            opacity=1,
            line=dict(color='red',
                      width=4),
            showlegend=True
        ))
    # adding the smoothed trace
    fig.add_trace(
        go.Scatter(
            x=df.date,
            y=df[var_new+'_smooth'],
            name='smoothed',
            mode='lines',
            opacity=1,
            line=dict(color='purple',
                      width=6),
            showlegend=True
        ))
    fig.update_layout(
        xaxis={'title': None,
               'titlefont': {'color': '#BFBFBF',
                             'family': font_use},
               'tickfont': {'color': '#002A34',
                            'size': 20,
                            'family': font_use},
               'gridcolor': '#4A4A4A',
               'linecolor': '#000000',
               'showgrid': False},
        yaxis={'title': 'Digital Demand',
               'titlefont': {'color': '#002A34',
                             'size': 30,
                             'family': font_use},
               'tickfont': {'color': '#002A34',
                            'size': 20,
                            'family': font_use},
               'showgrid': False,
               'zeroline': False},
        margin={'l': 100,
                'b': 150,
                't': 10,
                'r': 40},
        title={'text': f'{text}'.capitalize(),
               'font': {'color': '#000000',
                        'size': 30,
                        'family': font_use},
               'yanchor': "top",
               'xanchor': "center"},
        legend={'font': {'size': 15,
                         'color': '#333',
                         'family': font_use},
                'yanchor': "top",
                'xanchor': "center",
                'y': 0.9,
                'x': .85,
                'orientation': 'v',
                },
        template='none',
        hovermode='x unified',
        plot_bgcolor='#F0F0F0',
        # width=1920,
        height=720,
    )

    return fig.to_html()

graphs =[]

for text in texts:
    df_raw = df
    fig_html = single(
            df_raw,
            key=text.lower(),  # Pass the lowercase text directly
            geo='DE',
            cat=13,
            startdate=init_date,
            index=True,
            indexdate=index_date,
            font_use='Arial, sans-serif',
            out_type=output_type
        )
    graphs.append(fig_html)

@blueprint.route('/index')
@login_required
def index():
    # Call your single function here or modify the code accordingly
    # text = session.get('text', 'vodafone')
    # df_raw = df
    # plot_div = single(
    #     df_raw,
    #     key=text.lower(),  # Pass the lowercase text directly
    #     geo='DE',
    #     cat=13,
    #     startdate=init_date,
    #     index=True,
    #     indexdate=index_date,
    #     font_use='Arial, sans-serif',
    #     out_type=output_type
    # )

    return render_template('home/index.html', segment='index', plot_div=graphs)



# ...

@blueprint.route('/update_text', methods=['POST'])
@login_required
def update_text():
    global text  # Use the global variable to update the value
    value = request.form.get('value')  # Get the selected value from the request
    text = value.lower()  # Update the text variable with the selected value
    return jsonify({'message': 'Text updated successfully'})


@blueprint.route('/<template>')
@login_required
def route_template(template):
    try:
        if not template.endswith('.html'):
            template += '.html'

        # Detect the current page
        segment = get_segment(request)

        # Serve the file (if exists) from app/templates/home/FILE.html
        return render_template("home/" + template, segment=segment)

    except TemplateNotFound:
        return render_template('home/page-404.html'), 404

    except:
        return render_template('home/page-500.html'), 500


# Helper - Extract current page name from request
def get_segment(request):
    try:
        segment = request.path.split('/')[-1]

        if segment == '':
            segment = 'index'

        return segment

    except:
        return None
