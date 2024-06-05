import pandas as pd
import json
import numpy as np
import requests
import streamlit as st
import datetime as dt
from accounting_service_payments_applications import get_bearer_token,create_headers
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)


st.set_page_config('Accounting Services - QBO',
                    page_icon= ':factory:',
                    layout= 'wide'
                    )

st.title(':orange[AS - QBO] Automation Service :factory:')


def logs_AS_transactios(startDate,endDate,headers):
    json_data = {
        'operationName': 'getAccountingAPIFetchJournalsForDates',
        'variables': {
            'input': {
                        'startDate': startDate + 'T00:00:00-08:00',
                        'endDate': endDate + 'T23:59:59-08:00'
                },
            },
        'query': 'query getAccountingAPIFetchJournalsForDates($input: FetchJournalsForDatesInput!) {\n  getAccountingAPIFetchJournalsForDates(input: $input) {\n    journals {\n      id\n      createdAt\n      updatedAt\n      deletedAt\n      notes\n      oldCustomDate\n      newCustomDate\n      orderNumber\n      data\n      changeTag\n      reportedCount\n    }\n  }\n}\n'
    }
        
    response = requests.post('https://api.nabis.com/graphql/admin', headers=headers, json=json_data)

    data = response.json()
    return data


def creation_logs(data):
    df = pd.DataFrame(data['data']['getAccountingAPIFetchJournalsForDates']['journals'])
    applications = [ {'changeTag':i['changeTag'], 'createdAt':i['createdAt'] , 'Data':i['data']} for i in data['data']['getAccountingAPIFetchJournalsForDates']['journals']]

    df_applications = pd.DataFrame(applications)

    return df_applications


def payments_creation_logs(df_applications):
    payment_creation = df_applications.loc[df_applications['changeTag'].str.contains('unapplied')]

    df_pmt_creation = [i['transaction'] for idx,i in enumerate(payment_creation['Data']) ]
    createdAt_list = [i for i in payment_creation['createdAt']]
    changeTag_list = [i for i in payment_creation['changeTag']]

    list_pmt_created = []
    for idx,item in enumerate(df_pmt_creation):
        item['createdat'] = createdAt_list[idx]
        item['changeTag'] = changeTag_list[idx]
        list_pmt_created.append(item)


    payment_creation_data = pd.DataFrame(list_pmt_created)
    payment_creation_data = payment_creation_data.loc[~payment_creation_data['changeTag'].str.contains('meta')]

    payment_creation_data['bank_account'] = np.where(((payment_creation_data['method']=='CHECK') | (payment_creation_data['method']=='EFT') ) & (payment_creation_data['originCompany']=='NABITWO'), 473,
                                        np.where((payment_creation_data['method']=='EFT') & (payment_creation_data['originCompany']=='NABIFIVE'),469,
                                                 np.where((payment_creation_data['method']=='CHECK') & (payment_creation_data['originCompany']=='NABIFIVE'),61,
                                                          np.where((payment_creation_data['method']=='CASH') & (payment_creation_data['location']=='CASH_IN_LA'),475,
                                                                   np.where((payment_creation_data['method']=='CASH') & (payment_creation_data['location']=='CASH_IN_OAK'),474,
                                                                            np.where((payment_creation_data['method']=='CASH') & (payment_creation_data['location']=='CASH_IN_WOODLAKE'),486, np.nan
                                        ))))))

    
    payments_df = payment_creation_data.loc[(payment_creation_data['type']=='PAYMENT') & (payment_creation_data['qbCustomerPaidById']!='6045') | (payment_creation_data['qbCustomerPaidById']!='1701')]
    self_collected_df = payment_creation_data.loc[payment_creation_data['type']=='SELF_COLLECTED']
    writeoff_df = payment_creation_data.loc[payment_creation_data['type']=='WRITE_OFF_EXTERNAL']
    nabis_creditMemos_df = payment_creation_data.loc[payment_creation_data['type']=='NABIS_CREDIT_MEMO']

    return payments_df,self_collected_df,writeoff_df,nabis_creditMemos_df


def submit_payment_creation(df_to_submit):
    payments_created_json = df_to_submit.reset_index(names='Index_ID').to_json(orient='records')
    data_json = {'data':payments_created_json}
    data_json_pmts_creation = json.dumps(data_json) 
    #wenhook_pmt_creation = 'https://hook.us1.make.com/u9202iqqgs1lv8escbyh4xbk17pmaxjd'
    webhook_pmt_creation_nabifive = 'https://hook.us1.make.com/d3333qpq8dhhhcram45woqkk7enstdjr'
    response = requests.post(webhook_pmt_creation_nabifive,data=data_json_pmts_creation,headers={'Content-Type': 'application/json'})

    return response


def payment_application_data(df_applications):
    payment_applied = df_applications.loc[df_applications['changeTag'].str.startswith('applied')]

    df_pmt_applied = [i['transaction'] for i in payment_applied['Data']]
    createdAt_list = [i for i in payment_applied['createdAt']]
    changeTag_list = [i for i in payment_applied['changeTag']]

    list_payments_applied = []
    for idx,item in enumerate(df_pmt_applied):
        item['createdat'] = createdAt_list[idx]
        item['changeTag'] = changeTag_list[idx]
        list_payments_applied.append(item)

    payment_applied_data = pd.DataFrame(list_payments_applied)
    payment_applied_data = payment_applied_data.loc[~payment_applied_data['changeTag'].str.contains('meta')]
    
    method_mapping = {'CHECK': 3,
                  'CASH': 2,
                  'EFT':6}

    
    payment_applied_data['Method_ID'] = payment_applied_data['method'].map(method_mapping)
    applied_noOrderProvided_data = payment_applied_data.loc[payment_applied_data['orderNumber'].isnull()]
    nabione_payments_df = payment_applied_data.loc[(payment_applied_data['qbCustomerPaidById']=='6045') | (payment_applied_data['qbCustomerPaidById']=='1701')].copy()
    nabione_payments_df = nabione_payments_df.loc[~nabione_payments_df['orderNumber'].isna()]
    payment_applied_df = payment_applied_data.loc[(payment_applied_data['type']=='PAYMENT') & (~payment_applied_data['orderNumber'].isnull())].copy()
    payment_applied_df = payment_applied_df.loc[(~payment_applied_df['Method_ID'].isnull())].copy()
    payment_applied_df = payment_applied_df.loc[payment_applied_df['qbCustomerPaidById']!='6045'].copy()
    payment_applied_df[['orderNumber','Method_ID']] = payment_applied_df[['orderNumber','Method_ID']].astype(int)
    nabione_payments_df[['orderNumber','Method_ID']] = nabione_payments_df[['orderNumber','Method_ID']].astype(int)

    write_off_applied_data = payment_applied_data.loc[payment_applied_data['changeTag'].str.contains('WRITE')].copy()
    write_off_applied_data['orderNumber'] = write_off_applied_data['orderNumber'].replace([np.inf, -np.inf], np.nan).fillna(0)
    write_off_applied_data['orderNumber'] = write_off_applied_data['orderNumber'].astype(int)


    return payment_applied_df,applied_noOrderProvided_data,nabione_payments_df,write_off_applied_data


def submit_payment_application(payment_applied_data):
    payment_applied_json = payment_applied_data.reset_index(names='Index_ID').to_json(orient='records')
    data_json = {'data':payment_applied_json}
    data_json_pmts_applied = json.dumps(data_json)
    #wenhook_pmt_creation = 'https://hook.us1.make.com/74projknbpaur0175659mmdfz8j5ouwu'
    webhook_pmt_application_nabifive = 'https://hook.us1.make.com/o536jj2c0wx2y6bgmrcid8ysb9cfars5'
    response = requests.post(webhook_pmt_application_nabifive,data=data_json_pmts_applied,headers={'Content-Type': 'application/json'})
    
    return response


def remittance_report(df_applications):
    remittance = df_applications.loc[df_applications['changeTag'].str.contains('remittance_acceptance')]

    df_remittance = [pd.DataFrame(i['remittances']) for i in remittance['Data']]
    createdAt_list = [i for i in remittance['createdAt']]
    changeTag_list = [i for i in remittance['changeTag']]


    try:
        for idx,item in enumerate(df_remittance):
            item['createdat'] = createdAt_list[idx]
            item['changeTag'] = changeTag_list[idx]

        remittance_report = pd.concat(df_remittance)
        
    except:
        remittance_report = pd.DataFrame(df_remittance)
        pass

    if len(df_remittance) >0:    
        deductions_report = remittance_report.loc[remittance_report['type']=='DEDUCTION']
    else:
        deductions_report = pd.DataFrame()    

    return remittance_report,deductions_report


def rollback_data(df_applications):
    rollback = df_applications.loc[df_applications['changeTag'].str.startswith('rollback')]

    df_rollback = [i['transaction'] for i in rollback['Data']]
    createdAt_list = [i for i in rollback['createdAt']]
    changeTag_list = [i for i in rollback['changeTag']]

    list_rollback_created = []
    for idx,item in enumerate(df_rollback):
        item['createdat'] = createdAt_list[idx]
        item['changeTag'] = changeTag_list[idx]
        list_rollback_created.append(item)

    rollback_creation_data = pd.DataFrame(list_rollback_created)

    return rollback_creation_data


def pending_deductions(df_applications):
    pending_deductions = df_applications.loc[df_applications['changeTag'].str.contains('pending-deduction-creation')]

    df_pending_deduction = [i['pendingDeduction'] for i in pending_deductions['Data']]
    createdAt_list = [i for i in pending_deductions['createdAt']]
    changeTag_list = [i for i in pending_deductions['changeTag']]

    list_pending_deductions = []
    for idx,item in enumerate(df_pending_deduction):
        item['createdat'] = createdAt_list[idx]
        item['changeTag'] = changeTag_list[idx]
        list_pending_deductions.append(item)

    pending_deductions_data = pd.DataFrame(list_pending_deductions)

    manual_invs_deductions = pending_deductions_data.loc[pending_deductions_data['orderNumber'].isna()].copy()
    
    pending_deductions_data_grp = pending_deductions_data.groupby('orderNumber').agg({'amount':'sum','eligbleAt':'first','qbCustomerPaidById':'first','qbClassInvoiceBrandOrg':'first','qbCustomerPaidToId':'first','createdat':'first','changeTag':'first','invoiceNumber':'first'}).reset_index()
    columns_names = pending_deductions_data_grp.columns.tolist()

    manual_invs_deductions = manual_invs_deductions[columns_names]
    deductions_df = pd.concat([pending_deductions_data_grp,manual_invs_deductions])
    

    return deductions_df,pending_deductions_data


def submit_deductions_application(deductions_data):
    deductions_json = deductions_data.reset_index(names='Index_ID').to_json(orient='records')
    data_json = {'data':deductions_json}
    data_json_pmts_applied = json.dumps(data_json)
    #wenhook_pmt_creation = 'https://hook.us1.make.com/92h5u32aya4rimaqhtaji73ihc4csvbd'
    webhook_pending_deductions_nabifive = 'https://hook.us1.make.com/5lilbjljxwy2qxlyhqfrkd5zwyxnd29w'
    response = requests.post(webhook_pending_deductions_nabifive,data=data_json_pmts_applied,headers={'Content-Type': 'application/json'})
    
    return response


def submit_noOrders_application(no_orders_data):
    noOrders_json = no_orders_data.reset_index(names='Index_ID').to_json(orient='records')
    data_json = {'data':noOrders_json}
    data_json_noOrders = json.dumps(data_json)
    #webhook_noOrders = 'https://hook.us1.make.com/5y2ttxx8cjzti2emyfpstu2uu0adglu3'
    webhook_manuals_nabifive = 'https://hook.us1.make.com/eo46ccn7y81qkaiwy90wnshvoycj9pms'
    response = requests.post(webhook_manuals_nabifive, data=data_json_noOrders, headers={'Content-Type': 'application/json'})

    return response


def submit_write_off(write_off_data):
    write_off_json = write_off_data.reset_index(names='Index_ID').to_json(orient='records')
    data_json = {'data':write_off_json}
    data_json_write_off = json.dumps(data_json)
    webhook_write_off = 'https://hook.us1.make.com/s8n25vyenozx2vywtvi98iv2ux54hi8a'
    response = requests.post(webhook_write_off,data=data_json_write_off,headers={'Content-Type': 'application/json'})
    
    return response


def submit_nabione(nabione_data):
    nabione_json = nabione_data.reset_index(names='Index_ID').to_json(orient='records')
    data_json = {'data':nabione_json}
    data_json_nabione = json.dumps(data_json)
    #webhook_nabione = 'https://hook.us1.make.com/crfgdh7qu23otm6a91hbsvh52ajagfvu'
    webhook_nabione_nabifive = 'https://hook.us1.make.com/1ktcl3rva8umuh56gzqug4dz0ngbcsxg'
    response = requests.post(webhook_nabione_nabifive,data=data_json_nabione,headers={'Content-Type': 'application/json'})
    
    return response


st.cache()
def filter_dataframe(df: pd.DataFrame,key) -> pd.DataFrame:
        """
        Adds a UI on top of a dataframe to let viewers filter columns

        Args:
            df (pd.DataFrame): Original dataframe

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        modify = st.checkbox("Add filters",value=False,key=key)

        if not modify:
            return df

        df = df.copy()

        key2 = key + '_'
        
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
            to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
            for column in to_filter_columns:
                left, right = st.columns((1, 20))
                # Treat columns with < 10 unique values as categorical
                if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                    user_cat_input = right.multiselect(
                        f"Values for {column}",
                        df[column].unique(),
                        default=list(df[column].unique()),
                    )
                    df = df[df[column].isin(user_cat_input)]
                elif is_numeric_dtype(df[column]):
                    _min = float(df[column].min())
                    _max = float(df[column].max())
                    step = (_max - _min) / 100
                    user_num_input = right.slider(
                        f"Values for {column}",
                        min_value=_min,
                        max_value=_max,
                        value=(_min, _max),
                        step=step,
                    )
                    df = df[df[column].between(*user_num_input)]
                elif is_datetime64_any_dtype(df[column]):
                    user_date_input = right.date_input(
                        f"Values for {column}",
                        value=(
                            df[column].min(),
                            df[column].max(),
                        ),
                    )
                    if len(user_date_input) == 2:
                        user_date_input = tuple(map(pd.to_datetime, user_date_input))
                        start_date, end_date = user_date_input
                        df = df.loc[df[column].between(start_date, end_date)]
                else:
                    user_text_input = right.text_input(
                        f"Write {column} Name",key= f'{key}_text_widget'
                    )

                    user_text_input = user_text_input.split()
                    user_text_input
                    if user_text_input:
                        df = df[df[column].isin(user_text_input)]
                        
        
        return df




with st.form(key='log_in',):

    email = st.text_input('email:'),
    password_st = st.text_input('Password:',type='password')

    submitted = st.form_submit_button('Log in')

try:
    if submitted:
        st.write('Credentials Saved')


        user = email[0]
        password = password_st
        token,user_id = get_bearer_token(user,password)
        headers = create_headers(token)
        st.session_state['headers'] = headers
        
except:
    st.warning('Incorrect Email or Password, Try again')



if submitted:
    st.session_state['initialize'] = 'initialize'


if "initialize" not in st.session_state:
    st.write('Enter Your Credentials and Generate Dataframe')
else:

    st.markdown(':red[DO NOT RUN MULTIPLE DATA FRAME SUBMISSIONS AT ONCE!]')
    with st.expander('Instructions'):
        st.text('Select the range of dates you want to work with')
        st.text('Click on the submit button to send the instructions to make to start performing the applications in QBO')
        st.text('Review the google sheets file provided to ensure all the applications were perform')
        st.text('If the application stops and the transactions are incomplete, retrieve the index number of the last recorded row and input it below. Transactions will start from the next unrecorded index.')
        
        st.markdown('***')
        
        st.subheader('Map of tabs in google sheets')
        st.write('Tab "Data" --> Contains the Payment Creation Information.')
        st.write('Tab "Application_Data" --> Contains payments applied to fee invoice.')
        st.write('Tab "JE_applications" --> Contains Journal Entries for due to consignor.')
        st.warning('Tab Application_Data and JE_applications are for the same dataframe, you should look for the largest index number on both tabs')
        st.write('Tab "Deductions" --> Contains Journal Entries for Deductions.')
        st.write('Tab "AR Holding JE" --> Contains Journal Entries for Deductions where invoice does not exist in QBO.')
        st.write('Tab "Nabione_transactions" --> Contains Journal Entries for short payments transactions.')
        st.write('Tab "Manual Invoices Applications" --> Contains Journal Entries for manual invoices and/or nabione.')
        st.markdown('***')

  
    startDate = st.date_input("Select Start Date",value=dt.datetime.today(),format="YYYY-MM-DD",key='start')
    endDate = st.date_input("Select End Date",value=dt.datetime.today(),format="YYYY-MM-DD",key='end')
    startDate = str(startDate)
    endDate = str(endDate)

    as_logs = logs_AS_transactios(startDate,endDate,st.session_state['headers'])

    st.cache_data()
    df_applications = creation_logs(as_logs)
    st.session_state['df_applications'] = df_applications

    payment_creation_data,self_collected,write_off,nabis_credit_memo = payments_creation_logs(st.session_state['df_applications'])
    payments_application_data,applied_noOrderProvided_data,nabione_df,write_off_applied_data = payment_application_data(st.session_state['df_applications'])
    remittances_report,deductions_report = remittance_report(st.session_state['df_applications'])
    rollback_df = rollback_data(st.session_state['df_applications'])
    pending_deductions_df,pending_deductions_data = pending_deductions(st.session_state['df_applications'])

    st.warning('Be careful before sending data to QBO ensure you will not duplicate any item', icon="⚠️")

    st.text(f"Payments DataFrame for period from {startDate} to {endDate}")

    
    payment_creation_data = payment_creation_data.loc[~payment_creation_data['bank_account'].isna()].copy()
    

    csv_payments = payment_creation_data.to_csv().encode('utf-8')
    

    user_input_creation = st.number_input('Index Number',min_value=0,value=0,key='pmt_creation')

    payment_creation_data = filter_dataframe(payment_creation_data,key='create_payment')
    if int(user_input_creation) == 0:
        payment_creation_data
    else:
        index_creation = int(user_input_creation)
        payment_creation_data = payment_creation_data[payment_creation_data.index.get_loc(index_creation)+1:]
        payment_creation_data

    
    st.download_button('Download Payments Data',data=csv_payments,file_name='Payments.csv',mime='text/csv')    
    
    submit_button = st.button('Send Payments to QBO',key='creation')
    if submit_button:
        payment_creation_data['appliedAt'] = pd.to_datetime(payment_creation_data['appliedAt'])
        payment_creation_data['createdat'] = pd.to_datetime(payment_creation_data['createdat'])
        response = submit_payment_creation(payment_creation_data)
    
        st.write('Request sent to Make automation, review google sheet below for JE logs')
        st.link_button('Go to Google Sheet','https://docs.google.com/spreadsheets/d/1yLJN4SqmoDd_7glsz6R01q6o5sHY8qjleqwp3e-o9Ns/edit#gid=0')

    st.markdown('---')

    st.success('Once the JEs in QBO are created, submit the applications',icon="🚨")
    st.text(f"Payments Application DataFrame for period from {startDate} to {endDate}")
    

    csv_payments_applications = payments_application_data.to_csv().encode('utf-8')
    

    st.text('Filter DataFrame If the app stops and the applications are incompleted, Pull the Index number from the google sheet available in Column called "Index_Dataframe"')
    user_input = st.number_input('Index Number',min_value=0,value=0,key='pmt_application')

    payments_application_data = filter_dataframe(payments_application_data,key='filter_applications')    
    if int(user_input) == 0:
        payments_application_data
    else:
        index_input = int(user_input)
        payments_application_data = payments_application_data[payments_application_data.index.get_loc(index_input)+1:]
        payments_application_data

    st.download_button('Download Payments Application Data',data=csv_payments_applications,file_name='Payments_applications.csv',mime='text/csv')    

    submit_applications = st.button('Submit Payment Applications')

    if submit_applications:
        payments_application_data['appliedAt'] = pd.to_datetime(payments_application_data['appliedAt'])
        payments_application_data['createdat'] = pd.to_datetime(payments_application_data['createdat'])
        response = submit_payment_application(payments_application_data)
        st.write('Request sent to Make automation, review google sheet below for JE logs')
        st.link_button('Go to Google Sheet','https://docs.google.com/spreadsheets/d/1yLJN4SqmoDd_7glsz6R01q6o5sHY8qjleqwp3e-o9Ns/edit#gid=0')    


    st.markdown('---')
    st.text('Nabione Payments Created, Dataframe')
    
    csv_nabione = nabione_df.to_csv().encode('utf-8')
    

    user_input_nabione = st.number_input('Index Number',min_value=0,value=0,key='nabione')
    nabione_df = filter_dataframe(nabione_df,key='key_2')
  
    if int(user_input_nabione) == 0:
        nabione_df
    else:
        index_nabione = int(user_input_nabione)
        nabione_df = nabione_df[nabione_df.index.get_loc(index_nabione)+1:]
        nabione_df    

    st.download_button('Download Nabione Payments Created Data',data=csv_nabione,file_name='Nabione_Payments_Created.csv',mime='text/csv')

    nabione_button = st.button("Submit Nabione Transactions")

    if nabione_button:
        nabione_df['appliedAt'] = pd.to_datetime(nabione_df['appliedAt'])
        nabione_df['createdat'] = pd.to_datetime(nabione_df['createdat'])
        response = submit_nabione(nabione_df)
        st.write('Request sent to Make automation, review google sheet below for JE logs')
        st.link_button('Go to Google Sheet','https://docs.google.com/spreadsheets/d/1yLJN4SqmoDd_7glsz6R01q6o5sHY8qjleqwp3e-o9Ns/edit#gid=0')    



    st.markdown('---')
    st.text('Payments Applied to Manual invoices')
    
    csv_payments_applications_noOrder = applied_noOrderProvided_data.to_csv().encode('utf-8')

    user_input_noOrders = st.number_input('Index Number',min_value=0,value=0,key='noOrder')
    applied_noOrderProvided_data = filter_dataframe(applied_noOrderProvided_data,key = 'filter_Manuals')
  
    if int(user_input_noOrders) == 0:
        applied_noOrderProvided_data
    else:
        index_noOrders = int(user_input_noOrders)
        applied_noOrderProvided_data = applied_noOrderProvided_data[applied_noOrderProvided_data.index.get_loc(index_noOrders)+1:]
        applied_noOrderProvided_data

    submit_noOrders = st.button('Submit No orders applications')
    if submit_noOrders:
        applied_noOrderProvided_data['appliedAt'] = pd.to_datetime(applied_noOrderProvided_data['appliedAt'])
        applied_noOrderProvided_data['createdat'] = pd.to_datetime(applied_noOrderProvided_data['createdat'])
        response = submit_noOrders_application(applied_noOrderProvided_data)
        st.write('Request sent to Make automation, review google sheet below for JE logs')
        st.link_button('Go to Google Sheet','https://docs.google.com/spreadsheets/d/1yLJN4SqmoDd_7glsz6R01q6o5sHY8qjleqwp3e-o9Ns/edit#gid=0')  

    st.download_button('Download Payments Application No Orders Data',data=csv_payments_applications_noOrder,file_name='Payments_applications_noOrders.csv',mime='text/csv')


    st.markdown('---')
    st.text('Deductions Report DataFrame')
    
    csv_deductions = pending_deductions_df.to_csv().encode('utf-8')

    pending_deductions_df['eligbleAt'] = pd.to_datetime(pending_deductions_df['eligbleAt'])

    user_input_deductions = st.number_input('Index Number',min_value=0,value=0,key='deductions')
    pending_deductions_df = filter_dataframe(pending_deductions_df,key = 'filter_deductions')
  
    if int(user_input_deductions) == 0:
        pending_deductions_df
    else:
        index_deductions = int(user_input_deductions)
        pending_deductions_df = pending_deductions_df[pending_deductions_df.index.get_loc(index_deductions)+1:]
        pending_deductions_df    

    
    st.download_button('Download Deductions Data',data=csv_deductions,file_name='deductions.csv',mime='text/csv')
    
    submit_deductions = st.button('Submit Deductions')
    if submit_deductions:
        pending_deductions_df['eligbleAt'] = pd.to_datetime(pending_deductions_df['eligbleAt'])
        pending_deductions_df['createdat'] = pd.to_datetime(pending_deductions_df['createdat'])
        response = submit_deductions_application(pending_deductions_df)
        st.write('Request sent to Make automation, review google sheet below for JE logs')
        st.link_button('Go to Google Sheet','https://docs.google.com/spreadsheets/d/1yLJN4SqmoDd_7glsz6R01q6o5sHY8qjleqwp3e-o9Ns/edit#gid=0')    


    st.markdown('---')
    st.text('Rollback Report DataFrame')
    rollback_df
    csv_rollback = rollback_df.to_csv().encode('utf-8')
    st.download_button('Download Rollback Data',data=csv_rollback,file_name='Rollback.csv',mime='text/csv')    
    
