from urllib import response
from django.shortcuts import redirect, render
from django.shortcuts import HttpResponse
from django.contrib.auth.decorators import login_required
from django import forms
import pandas as pd
import snowflake.connector as sf
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from .pvi_form import pviForm
from .pvi_form import pri_drug,STATES,roll_up,HCP
from json import dumps
from django.http import HttpResponseRedirect
from functools import wraps
import time

def timer(func):
    """helper function to estimate view execution time"""

    @wraps(func)  # used for copying func metadata
    def wrapper(*args, **kwargs):
        # record start time
        start = time.time()

        # func execution
        result = func(*args, **kwargs)
        
        duration = (time.time() - start) * 1000
        # output execution time to console
        print('view {} takes {:.2f} ms'.format(
            func.__name__, 
            duration
            ))
        return result
    return wrapper

# Create your views here.
def home(request):

    return render(request,'home.html',{})



def userlogin(request):

    return render(request, 'userlogin.html',{})

def home_redirect(request):

    return render(request,'home_redirect.html',{})

@login_required
def admindashboard(request):

    return render(request, 'admindashboard.html')

@login_required
def adduser(request):

    return render(request, 'adduser.html')

@login_required
def pvi_results(request):

    return render(request, 'pvi_results.html')

@login_required
@timer
def pvi(request):
    market_basket =[]
    market_basket_string = ""
    states_list =[]
    states_string =""
    primary_drug_string =""
    primary_drug_list = []
    roll_up_speciality_string =""
    roll_up_speciality_list = []
    hcp_list = []
    hcp_string = ""
    form = pviForm()    
    list = dumps(pri_drug)
    Roll_up = dumps(roll_up)
    states = dumps(STATES)
    hcp = dumps(HCP)
    npi_list = "All"
    user = "swanand"
    message = "PLEASE SELECT CORRECT SET OF PRIMARY DRUGS AND MARKET BASKET"
    
    if (request.method =='POST'):
        form = pviForm(request.POST)

        
        cohort = request.POST['cohort_name']
        print(cohort)

        hcp_idx = request.POST['hcp_group']
        if(not hcp_idx):
            hcp_string = "All"
        else:
            hcp_split = hcp_idx.split(",") 
            for i in hcp_split:
                hcp_list.append(HCP[(int(i))])
            for i in hcp_list:
                hcp_string += ":" + i 
            hcp_string = hcp_string[1:]
        print(hcp_string)

        


        loopback = request.POST['loopback_period']
        print(loopback)
  
        states_idx = request.POST['state']
        if(not states_idx):
            states_string = "All"
        else:
            states_split = states_idx.split(",") 
            for i in states_split:
                states_list.append(STATES[(int(i))])
            for i in states_list:
                states_string += ":" + i 
            states_string = states_string[1:]
        print(states_string)

        roll_up_speciality_idx = request.POST['roll_up_speciality']
        if(not roll_up_speciality_idx):
            roll_up_speciality_string = "All"
        else:
            roll_up_speciality_split = roll_up_speciality_idx.split(",") 
            for i in roll_up_speciality_split:
                roll_up_speciality_list.append(pri_drug[(int(i))])
            for i in roll_up_speciality_list:
                roll_up_speciality_string += ":" + i 
            roll_up_speciality_string = roll_up_speciality_string[1:]
        print(roll_up_speciality_string)


        
        
        primary_drug_idx = request.POST['primary_drug']
        primary_drug_split = primary_drug_idx.split(",") 
        for i in primary_drug_split:
            primary_drug_list.append(pri_drug[(int(i))])
            for i in primary_drug_list:
                primary_drug_string += ":" + i 
            primary_drug_string = primary_drug_string[1:]
        print(primary_drug_string)

        market_basket_idx = request.POST['market_basket']
        market_basket_split = market_basket_idx.split(",") 
        for i in market_basket_split:
            market_basket.append(pri_drug[(int(i))])
        for i in market_basket:
            market_basket_string += ":" + i 
        market_basket_string = market_basket_string[1:]

        market_basket_string = market_basket_string +":" + primary_drug_string
        print(market_basket_string)


        conn = sf.connect(
            account = 'dn13102',
            user = 'TEMP_DJANGO',
            password = 'Django@2022',
            database = 'TEST',
            region ='us-east-1',
            warehouse = 'ETL_TEST',
            role = 'TEMP_DJANGO_ROLE',
            schema = 'ADW',        
        )
        # Create cursor
        cur = conn.cursor()
        cur.execute("INSERT INTO TEST.ADW.PVI_COHORTS_TAB (COHORT_NAME, PRIMARY_BRAND, MARKET_BRANDS,ROLLUP_SPECIALTY,HCP_GROUP,NPI_LIST,STATE,PERIOD,CREATED_BY)" "VALUES(%s, %s, %s, %s, %s , %s , %s , %s ,%s)" ,(cohort,primary_drug_string,market_basket_string,roll_up_speciality_string,hcp_string,npi_list,states_string,loopback,user))
        cur.execute("select hash(COHORT_NAME,PRIMARY_BRAND,MARKET_BRANDS) from TEST.ADW.PVI_COHORTS_TAB where (COHORT_NAME = %s)" ,(cohort))

        hash = cur.fetchone()
        hash = hash[0]
        print(hash)

        cur.execute("UPDATE TEST.ADW.PVI_COHORTS_TAB SET PVI_COHORT_ID = %s where (COHORT_NAME = %s) " ,(hash,cohort))
        cur.execute("call util_db.tools.CALCULATE_PVI_SCORE('TEST','INSIGHTSDWDEV', %s) ",(str(hash)))
        cur.execute("select NPI,PVI_SCORE from TEST.ADW.PVI_SCORES_TAB where (PVI_COHORT_ID = %s) " ,(str(hash)))
        data = []

        data = cur.fetchall()
        if(data):
            df = pd.DataFrame(data)
            #print(df)
            df.columns = ['NPI','PVI_SCORE']
        
            df['PVI_BINS'] = pd.cut(
            df['PVI_SCORE'], 
            [0,10,20,30,40,50,60,70,80,90,100], 
            labels=['0-10', '11-20', '21-30','31-40','41-50','51-60','61-70','71-80','81-90','91-100'],include_lowest=True
            )
            df.drop(['PVI_SCORE'], axis = 1,inplace=True)
            df = df.groupby('PVI_BINS').size().reset_index(name='NPI_COUNT') 
            print(df)
            df['NPI_COUNT_PERCENT'] = (df['NPI_COUNT'] / 
                      df['NPI_COUNT'].sum()) * 100
            df.drop(['NPI_COUNT'], axis = 1,inplace=True)
            pvi_bins_list = df['PVI_BINS'].tolist()
            npi_count_list = df['NPI_COUNT_PERCENT'].tolist()
            print(pvi_bins_list)
            print(npi_count_list)


        


        
        
            return render(request,'pvi_results.html',{'pvi_bins_list':pvi_bins_list,'npi_count_list':npi_count_list})
        else:
            return render(request,'pvi_results.html',{'message':message})


        

        
    
    return render(request,'pvi.html',{'form':form,'list':list,'states':states,'Roll_up':Roll_up,'hcp':hcp})

    

