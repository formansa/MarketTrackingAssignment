'''
Created on Mar 25, 2017

@author: Sam_I_Am

Spreadsheet url:
https://docs.google.com/spreadsheets/d/1Td8bFznAK-MFlkTDW9f6S6Z8zygCJ5x6d68tkI7HUis/edit#gid=0

'''

# imports for data
import quandl
import pandas as pd
import time
import datetime

# sheet read imports
from pprint import pprint
from googleapiclient import discovery
import os

# authorization imports
#from apiclient import discovery
#from oauth2client import client
#from oauth2client import tools
#from oauth2client.file import Storage
from oauth2client.client import GoogleCredentials



############
# GET DATA #
############

def get_qndl_data():
    # Create most recent Tuesday date:
    today_date =  pd.to_datetime('today')
    last_tuesday = today_date - pd.offsets.Week(weekday=1)
    # Quandl Key
    quandl.ApiConfig.api_key = 'eKx3zYEyks8Xyz2jjdph'

    # 13 Week T Bill Data
    data_tsry_bills = quandl.get("USTREASURY/BILLRATES.3", start_date=last_tuesday, end_date=last_tuesday)
    data_tsry_bills.rename(columns={'13 Wk Bank Discount Rate':'Value'}, inplace=True)


    # 10 YR Treasury Rate
    data_tsry_bonds = quandl.get("FRED/DGS10", start_date=last_tuesday, end_date=last_tuesday)
    #data_tsry_bonds.rename(columns={'VALUE':'Value'}, inplace=True)
    data_tsry_bonds.rename(columns={'DGS10':'Value'}, inplace=True)

    # Oil Barrel Price
    data_oilprice = quandl.get("CHRIS/CME_CL1.6", start_date=last_tuesday, end_date=last_tuesday)
    data_oilprice.rename(columns={'Settle':'Value'}, inplace=True)

    # NASDAQ
    data_NASDAQ = quandl.get("NASDAQOMX/COMP.1", start_date=last_tuesday, end_date=last_tuesday)
    data_NASDAQ.rename(columns={'Index Value':'Value'}, inplace=True)

    # Japanses Yen into USD
    data_YEN = quandl.get("BOE/XUDLJYD", start_date=last_tuesday, end_date=last_tuesday)

    # Euro into USD
    data_EURO = quandl.get("BOE/XUDLERD", start_date=last_tuesday, end_date=last_tuesday)
    #  reverse exchange rate to get USD into EURO
    data_EURO.iat[0,0] = (1/data_EURO.iat[0,0])

    # S&P 500 Data
    data_SandP = quandl.get("YAHOO/INDEX_GSPC.4", start_date=last_tuesday, end_date=last_tuesday)
    data_SandP.rename(columns={'Close':'Value'}, inplace=True)

    # Nikkei 225 Data
    data_Nikkei = quandl.get("YAHOO/INDEX_N225.4", start_date=last_tuesday, end_date=last_tuesday)
    data_Nikkei.rename(columns={'Close':'Value'}, inplace=True)

    # GE Stock Data
    data_GE = quandl.get("YAHOO/GE.4", start_date=last_tuesday, end_date=last_tuesday)
    data_GE.rename(columns={'Close':'Value'}, inplace=True)


    # Combine DataFrames!
    comp_data_list = [data_SandP, data_NASDAQ, data_Nikkei, data_EURO, data_YEN, data_oilprice, data_tsry_bills, data_tsry_bonds, data_GE]
    tracking_data_complete = pd.concat(comp_data_list, keys=['SP','NASDAQ','Nikkei','EUR/USD','USD/JPY','OIL','3MO TSRY','10YR TSRY','GE'])

    #remove date index
    tracking_data_complete.reset_index(level=1, drop=True, inplace=True)
    #convert to dict
    df_dict = tracking_data_complete.to_dict(orient='dict')



    return df_dict

# below function checks dictionary for missing elements that may result from changes or delays in updating the quandl API
# if missing items are found they are replaced with NA so the rest of the update can proceed
def check_formissing(check_input):
    dc = 0
    full_key_list = ['SP','NASDAQ','Nikkei','EUR/USD','USD/JPY','OIL','3MO TSRY','10YR TSRY','GE']

    while dc < 9:
        for i in full_key_list:
            try:
                check_input['Value'][i]

            except KeyError as errtxt:
                print("Missing Item: %s" %str(errtxt))
                missing_name = str(errtxt).replace("'", '')
                check_input['Value'][missing_name] = 'missing'
                dc += 1

            else:
                dc += 10

    return check_input



###############
# Append DATA #
###############

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

#api_key =  'AIzaSyB9riIBj0ZFWVR0Hwil8TUiEgmd7d1aH7g'
    # below line is used to set an ENVRIONMENTAL varible
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getcwd() + '/ServerClientSecret.json'
credentials = GoogleCredentials.get_application_default()
service = discovery.build('sheets', 'v4', credentials=credentials)
# , developerKey= api_key
spreadsheet_id = '1Td8bFznAK-MFlkTDW9f6S6Z8zygCJ5x6d68tkI7HUis' # API_Test_Sheet

active_row = 1
active_range = ('Sheet1!A%s:J%s' % (str(active_row), str(active_row)))


def append_new(df_dict):
    range_ = active_range
    value_input_option = 'RAW'
    insert_data_option = 'INSERT_ROWS'

    today_date2 =  pd.to_datetime('today')
    last_tuesday2 = today_date2 - pd.offsets.Week(weekday=1)
    lt_date_iso = last_tuesday2.isoformat()[0:10]

    add_vals = [[lt_date_iso],
                [df_dict['Value']['SP']],
                [df_dict['Value']['NASDAQ']],
                [df_dict['Value']['Nikkei']],
                [df_dict['Value']['EUR/USD']],
                [df_dict['Value']['USD/JPY']],
                [df_dict['Value']['OIL']],
                [df_dict['Value']['3MO TSRY']],
                [df_dict['Value']['10YR TSRY']],
                [df_dict['Value']['GE']]]

    value_rage_body = {
                        "range" : active_range,
                        "majorDimension" : 'COLUMNS',
                        "values" : add_vals
                        }

    request = service.spreadsheets().values().append(spreadsheetId = spreadsheet_id, range = range_, valueInputOption = value_input_option, insertDataOption = insert_data_option, body =  value_rage_body)
    response = request.execute()
    pprint(response)


# Runs on Wednesdays
if datetime.date.today().weekday() == 2:
    append_new(check_formissing(get_qndl_data()))


