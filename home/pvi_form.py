from django import forms
import pandas as pd
import numpy as np
import snowflake.connector as sf
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine




pri_drug=[]
STATES = []
roll_up = []
HCP = []

class pviForm(forms.Form):
    global pri_drug
    global STATES
    global roll_up
    global HCP

    conn = sf.connect(
            account = 'dn13102',
            user = 'SWANANDV',
            password = 'Dor@mon291999',
            database = 'TEST',
            region ='us-east-1',
            warehouse = 'ETL_TEST',
            role = 'QA',
            schema = 'BPH',        
        )
    sql = 'select distinct(BRAND_NAME) from test.bph.dim_brands_tab' 
        # Create cursor
    cur = conn.cursor()
        # Execute SQL statement
    cur.execute(sql)
        # Fetch result
        # print(cur.fetchall())
        # Create Dataframe
    data = []
    data = cur.fetchall()
    df = pd.DataFrame(data)
    df.columns = ['BRAND_NAME']
    #options = list(df.itertuples(index=True,name=None))

    pri_drug = df['BRAND_NAME'].tolist()


    conn.close()

    conn = sf.connect(
            account = 'dn13102',
            user = 'SWANANDV',
            password = 'Dor@mon291999',
            database = 'TEST',
            region ='us-east-1',
            warehouse = 'ETL_TEST',
            role = 'QA',
            schema = 'BPH',        
        )
    sql = '''select distinct(Rollup_specialty) from "TEST"."BPH"."DIM_SPECIALTIES_TAB" where Not Rollup_specialty = 'N/A'
    '''

    cur = conn.cursor()

    cur.execute(sql)

    data = []
    data = cur.fetchall()
    df = pd.DataFrame(data)
    df.columns = ['ROLL_UP']


    roll_up = df['ROLL_UP'].tolist()
    conn.close()

    conn = sf.connect(
            account = 'dn13102',
            user = 'SWANANDV',
            password = 'Dor@mon291999',
            database = 'TEST',
            region ='us-east-1',
            warehouse = 'ETL_TEST',
            role = 'QA',
            schema = 'BPH',        
        )
    sql = 'select distinct(STATE) from test.bph.dim_zips_tab' 

    cur = conn.cursor()

    cur.execute(sql)

    data = []
    data = cur.fetchall()
    df = pd.DataFrame(data)
    df.columns = ['STATES']


    STATES = df['STATES'].tolist()
    conn.close()

    conn = sf.connect(
            account = 'dn13102',
            user = 'SWANANDV',
            password = 'Dor@mon291999',
            database = 'TEST',
            region ='us-east-1',
            warehouse = 'ETL_TEST',
            role = 'QA',
            schema = 'BPH',        
        )
    sql = '''select distinct(HCP_TYPE) from TEST.BPH.DIM_HCP_TYPES where Not HCP_TYPE = 'N/A'
    '''

    cur = conn.cursor()

    cur.execute(sql)

    data = []
    data = cur.fetchall()
    df = pd.DataFrame(data)
    df.columns = ['HCP_TYPE']


    HCP = df['HCP_TYPE'].tolist()
    conn.close()


    
    



