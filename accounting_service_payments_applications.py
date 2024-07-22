import requests
import pandas as pd
import numpy as np
import datetime as dt
import streamlit as st
import gspread as gs
from google.oauth2 import service_account
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)


@st.cache_data
def get_bearer_token(user,password):
    user = user
    psswrd = password

    headers = {
    'authority': 'api.nabis.com',
    'accept': '*/*',
    'accept-language': 'es-ES,es;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://app.getnabis.com',
    'referer': 'https://app.getnabis.com/',
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    }


    json_data = {
    'operationName': 'SignIn',
    'variables': {
        'input': {
            'email': user,
            'password': psswrd,
            },
        },
        'query': 'mutation SignIn($input: LoginUserInput!) {\n  loginUser(input: $input) {\n    token\n    user {\n      ...userFragment\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment userFragment on User {\n  id\n  email\n  firstName\n  lastName\n  address1\n  address2\n  city\n  state\n  zip\n  phone\n  profilePicture\n  isAdmin\n  isDriver\n  driversLicense\n  __typename\n}\n',
    }   

    response = requests.post('https://api.nabis.com/graphql/admin', headers=headers, json=json_data)

    bearer_token = response.json()
    token = bearer_token['data']['loginUser']['token']
    user = bearer_token['data']['loginUser']['user']['id']
    
    return token,user


@st.cache_data
def refreshToken(token):
    json_data = {
        'operationName': 'RefreshToken',
        'variables': {
            'token': token,
            'organizationId': 'T3JnYW5pemF0aW9uOjk2MGY0YjU2LWM1ZGItNDFkNi04MjRkLTg1NjNlOThkODUwZQ==',
        },
        'query': 'mutation RefreshToken($token: String!, $organizationId: String) {\n  refreshToken(token: $token, organizationId: $organizationId) {\n    token\n    __typename\n  }\n}\n',
    }

    response = requests.post('https://api.nabis.com/graphql/admin', headers=headers, json=json_data)

    data = response.json()
    data = data['data']['refreshToken']['token']
    
    return data


@st.cache_data
def get_retailer_id(headers):


    json_data = {
    'operationName': 'AllAdminOrganizationsWithRetailers',
    'variables': {},
    'query': 'query AllAdminOrganizationsWithRetailers {\n  viewer {\n    allAdminOrganizationsWithRetailers {\n      id\n      name\n      doingBusinessAs\n      type\n      __typename\n    }\n    __typename\n  }\n}\n',
    }

    response = requests.post('https://api.nabis.com/graphql/admin', headers=headers, json=json_data)

    data_retailer_id = response.json()

    data = data_retailer_id['data']['viewer']['allAdminOrganizationsWithRetailers']

    return data


@st.cache_data
def create_payment(list_pmts,headers):
    # Request to create a payment

    for idx,pmt in enumerate(list_pmts):
        
        if pmt['Type'] == 'Payment':
            transactionType = 'PAYMENT'
        elif pmt['Type'] == 'Self_Collected':
            transactionType = 'SELF_COLLECTED'
        elif pmt['Type'] == 'Write_Off_Nabis':
            transactionType = 'WRITE_OFF_NABIS'
        elif pmt['Type'] == 'Write_Off_External':
            transactionType = 'WRITE_OFF_EXTERNAL'     
        
        id_retailer = pmt['Retailer_ID']
        payment_ref = pmt['Pmt_Ref']
        utc_str = 'T12:00:00.000Z'
        paidAt = pmt['Payment_Date'] + utc_str
        pmt_amount = pmt['pmt_Amount']
        notes = pmt['AdminNotes']
        if notes == None:
            notes = ''
        

        if transactionType == 'PAYMENT':
            pmt_method = str.upper(pmt['Pmt_Method'])
            
            if pmt_method == 'EFT':
                location = None
            elif str.upper(pmt['Location']) == 'WL':
                location = 'CASH_IN_WOODLAKE'
            else:
                location = 'CASH_IN_' + str.upper(pmt['Location'])
        
        elif transactionType == 'SELF_COLLECTED' or str(transactionType).startswith('WRITE'):
            pmt_method = None
            location = None

        if str(payment_ref).endswith('-RG'):
            anticipated_paid_to = ['TWluaW1hbE9yZ2FuaXphdGlvbkJhbmtpbmc6ZjVhOWE0ZDEtNDdmMC00MTQxLTlhYjktYjBiYjEzNzZmNDVj']
        else:
            anticipated_paid_to = [None]
                

        try:
            data_pmt_tid = get_pmt_transaction_number(headers, pmt)
        except:
            json_data = {
                'operationName': 'postAccountingAPIRecordTransaction',
                'variables': {
                    'input': {
                        'transactionType': transactionType,
                        'paidBy': id_retailer,
                        'name': payment_ref,
                        'paidAt': paidAt,
                        'amount': pmt_amount,
                        'method': pmt_method,
                        'location': location,
                        'adminNotes': notes,
                        'publicNotes': '',
                        'earmarkedOrganizationIds': anticipated_paid_to
                    },
                },
                'query': 'mutation postAccountingAPIRecordTransaction($input: PostAccountingAPIRecordTransactionInput!) {\n  postAccountingAPIRecordTransaction(input: $input) {\n    amount\n    id\n    name\n    number\n    __typename\n  }\n}\n',
            }

            response = requests.post('https://api.nabis.com/graphql/admin', headers=headers, json=json_data)

            data_pmt_created = response.json()

            data = data_pmt_created['data']['postAccountingAPIRecordTransaction']
            continue    

        
       
  

@st.cache_data
def get_pmt_transaction_number(headers,invs_list):
    # Request to get the Transaction ID for the payment created.

    reference = invs_list['Pmt_Ref']
   

    json_data = {
        'operationName': 'getAccountingAPIPaymentTransactions',
        'variables': {
            'input': {
                'pageInfo': {
                    'numItemsPerPage': 50,
                    'orderBy': [],
                    'page': 1,
                },
                'transactionNumber': None,
                'name': reference,
                'originalTransactionNumber': None,
                'afterPaidAtDate': None,
                'beforePaidAtDate': None,
                'afterAppliedAtDate': None,
                'beforeAppliedAtDate': None,
                'afterRemittedAtDate': None,
                'beforeRemittedAtDate': None,
                'paidBy': None,
                'location': None,
                'fromAccountId': None,
                'toAccountId': None,
                'invoicePaidBy': None,
                'invoicePaidTo': None,
            },
        },
        'query': 'query getAccountingAPIPaymentTransactions($input: GetAccountingAPIPaymentTransactionsInput!) {\n  getAccountingAPIPaymentTransactions(input: $input) {\n    payments {\n      transactionNumber\n      name\n      paidAt\n      appliedAt\n      remittedAt\n      adminNotes\n      publicNotes\n      transactionType\n      fromAccount\n      fromAccountType\n      toAccount\n      toAccountType\n      originalTransactionNumber\n      originalTransactionName\n      amount\n      balance\n      method\n      autoGenerated\n      paidByOrgName\n      rolledBackAt\n      totalDeductions\n      appliedDeductions\n      expectedDeductions\n      __typename\n    }\n    pageInfo {\n      page\n      numItemsPerPage\n      hasNextPage\n      orderBy {\n        attribute\n        order\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n',
    }

    response = requests.post('https://api.nabis.com/graphql/admin', headers=headers, json=json_data)

    data = response.json()
    pmt_transaction_num = data['data']['getAccountingAPIPaymentTransactions']['payments'][0]['transactionNumber']

    return pmt_transaction_num


@st.cache_data
def payment_application(item,headers):
        
        json_data = {
            'operationName': 'postAccountingAPIApplyTransaction',
            'variables': {
                'input': {
                    'originalTransactionNumber': item['pmt_tid'],
                    'newTransactions': item['applications'],
                    'updateTransactions': [],
                },
            },
            'query': 'mutation postAccountingAPIApplyTransaction($input: PostAccountingAPIApplyTransactionInput!) {\n  postAccountingAPIApplyTransaction(input: $input)\n}\n',
        }

        response = requests.post('https://api.nabis.com/graphql/admin', headers=headers, json=json_data)
        data = response.json()
        return data
    

@st.cache_data
def create_headers(token):

    headers = {
    'authority': 'api.nabis.com',
    'accept': '*/*',
    'accept-language': 'es-ES,es;q=0.9',
    'authorization': 'Bearer '+ token,
    'content-type': 'application/json',
    'origin': 'https://app.getnabis.com',
    'referer': 'https://app.getnabis.com/',
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    }

    return headers
    

@st.cache_data
def search_invoices(order,headers):
    json_data = {
        'operationName': 'GetAccountingAPIDetailedInvoicesByNumber',
        'variables': {
            'input': {
                'number': order,
            },
        },
        'query': 'query GetAccountingAPIDetailedInvoicesByNumber($input: GetAccountingAPIDetailedInvoicesByNumberInput!) {\n  getAccountingAPIDetailedInvoicesByNumber(input: $input) {\n    matchingOrderNumber {\n      invoiceNumber\n      orderNumber\n      invoiceGroupType\n      invoiceTypeName\n      brandName\n      retailerName\n      paidToName\n      paidByName\n      invoiceTotal\n      invoiceCollected\n      invoiceCollectedRemaining\n      __typename\n    }\n    matchingInvoiceNumber {\n      invoiceNumber\n      orderNumber\n      invoiceGroupType\n      invoiceTypeName\n      brandName\n      retailerName\n      paidToName\n      paidByName\n      invoiceTotal\n      invoiceCollected\n      invoiceCollectedRemaining\n      __typename\n    }\n    __typename\n  }\n}\n',
    }

    response = requests.post('https://api.nabis.com/graphql/admin', headers=headers, json=json_data)
    data = response.json()
    return data


st.cache_data
def filter_dataframe(df: pd.DataFrame,key) -> pd.DataFrame:
        """
        Adds a UI on top of a dataframe to let viewers filter columns

        Args:
            df (pd.DataFrame): Original dataframe

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        df = df.copy()
        
        # Try to convert datetimes into a standard format (datetime, no timezone)
        for col in df.columns:
            if is_object_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception:
                    pass

            if is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        modification_container = st.container()

        with modification_container:
                if is_datetime64_any_dtype(df['Date']):
                    user_date_input = st.date_input(
                        f"Values",
                        value=(
                            df['Date'].max() - pd.Timedelta(days=1),
                            df['Date'].max() - pd.Timedelta(days=1),
                        ),
                    )
                    if len(user_date_input) == 2:
                        user_date_input = tuple(map(pd.to_datetime, user_date_input))
                        start_date, end_date = user_date_input
                        df = df.loc[df['Date'].between(start_date, end_date)]
            
        return df



st.cache()
def read_gs_byID(gs_ID,ws_ID):
    # Scopes links used to connect the script to the Google Drive and Google sheets
    scope = ['https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets']

    # Creating the credentials variable to connect to the API
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"],scopes=scope)

    # Passing the credentias to gspread
    client = gs.authorize(credentials=credentials)

    # Opening Google sheet using the ID
    google_sheet = client.open_by_key(gs_ID)


    # Method Worksheets gets the list of Tabs in the google sheet
    tab_name = google_sheet.worksheets()
    
    # open the sheet and get the data
    sheet = google_sheet.get_worksheet_by_id(ws_ID)
    # data = sheet.get_all_values()
    data = sheet.batch_get(['A:X'])
    return data


st.cache()
def df_cash():
    data_cash_log = read_gs_byID(st.secrets["gs_ID"]["cash_log_ID"],1792079758)
    cashlog_complete = pd.DataFrame(data_cash_log[0][1:],columns=data_cash_log[0][0])
    cashlog = cashlog_complete[['Date','Payment Reference','Amount','Invoices','Invoice Amt', 'Brand', 'Retailer', 'Amount Applied','Nabis Status']].copy()

    return cashlog_complete,cashlog


st.cache()
def df_checks():
    data_check_log = read_gs_byID(st.secrets["gs_ID"]["check_log_ID"],813380796)
    checklog_complete = pd.DataFrame(data_check_log[0][1:],columns=data_check_log[0][0])
    checklog_complete = checklog_complete.loc[checklog_complete['Company']!='Siban'].copy()
    checklog = checklog_complete[['Date','Payment Reference','Check Amount','Invoices','Invoice Amt', 'Brand', 'Retailer', 'Amount Applied','Nabis Status','QB Status']].copy()
    checklog.rename(columns={'Check Amount':'Amount'},inplace=True)
    #checklog['Payment Reference'] = checklog['Payment Reference'].astype('str')
    #checklog['Payment Reference'] = checklog['Payment Reference'].str.replace('([^\s\d])','',regex=True)
    checklog['Payment Reference'] = np.where(checklog['Payment Reference']=='',np.nan,checklog['Payment Reference'])
    checklog['Payment Reference'].fillna(method='ffill',inplace=True)

    return checklog_complete,checklog


st.cache()
def df_eft():
    data_eft_log = read_gs_byID(st.secrets["gs_ID"]["eft_log_ID"],2020060949)
    eftlog_complete = pd.DataFrame(data_eft_log[0][1:],columns=data_eft_log[0][0])
    colnames = ['Date','Payment Reference','Amount','Invoices','Invoice Amt', 'Brand', 'Retailer', 'Amount Applied','Nabis Status']
    eftlog = eftlog_complete[['Date','Payment Reference','Transfer Amount','Invoices','Invoice Amt', 'Brand', 'Retailer', 'Amount Applied','Nabis Status']]
    eftlog.rename(columns={'Transfer Amount':'Amount'},inplace=True)

    return eftlog_complete,eftlog



st.cache()
def logs_consolidated(cashlog,checklog,eftlog):
# Concatenating the 3 logs together and standirizing the data
    logs_concatenated = pd.concat([cashlog,checklog,eftlog])
    logs_concatenated = logs_concatenated.iloc[1:].copy()
    logs_concatenated_filter = logs_concatenated.loc[~logs_concatenated['Invoices'].str.strip().isin(['', '-','TEST','RR','RE','AA','PI_-PD'])].copy()
    logs_concatenated_filter['Pmt_Method'] = np.where(logs_concatenated_filter['Payment Reference'].str.contains('Cash'),'Cash',np.where(logs_concatenated_filter['Payment Reference'].str.contains('EFT'),'EFT','Check'))
    logs_concatenated_filter = logs_concatenated_filter.loc[logs_concatenated_filter['Invoices']!='Invoices'].copy()
    logs_concatenated_filter['Invoices'] = np.where(logs_concatenated_filter['Invoices'].str.contains('Multiple') , 'OP',logs_concatenated_filter['Invoices'])
    logs_concatenated_filter[['Amount','Invoice Amt','Amount Applied']] = logs_concatenated_filter[['Amount','Invoice Amt','Amount Applied']].apply(lambda x: x.str.strip())
    logs_concatenated_filter[['Amount','Invoice Amt','Amount Applied']] = logs_concatenated_filter[['Amount','Invoice Amt','Amount Applied']].apply(lambda x: x.str.replace('$',''))
    logs_concatenated_filter[['Amount','Invoice Amt','Amount Applied']] = logs_concatenated_filter[['Amount','Invoice Amt','Amount Applied']].apply(lambda x: x.str.replace(',',''))
    logs_concatenated_filter[['Invoice Amt','Amount Applied']] = logs_concatenated_filter[['Invoice Amt','Amount Applied']].apply(lambda x: x.str.replace('-','0'))
    logs_concatenated_filter['Amount'] = pd.to_numeric(logs_concatenated_filter['Amount'],errors='ignore')
    logs_concatenated_filter['Invoice Amt'] = pd.to_numeric(logs_concatenated_filter['Invoice Amt'],errors='ignore')
    logs_concatenated_filter['Amount Applied'] = pd.to_numeric(logs_concatenated_filter['Amount Applied'],errors='ignore')
    logs_concatenated_filter['Date'] = pd.to_datetime(logs_concatenated_filter['Date'])

    return logs_concatenated_filter


