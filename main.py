#!/usr/bin/env python
# coding: utf-8

#### import module ####

import pandas as pd
#import numpy as np
from sql_connection import SqlConnection
from set_datatablename import get_Data_Questions_Responses
from freq import frekans
from cross_freq import kirilimli_frekans

conn = SqlConnection()


def run_func(project_label, uniqueid, query=None, weight_column=None, cross_filter=None):
    result = []  # append edilecek boş bir liste yaratılıyor.
    pd.options.mode.chained_assignment = None
    selected_columns = ["FormId", "ColumnName", "ColumnValue",
                        "ColumnValueText", "weight", "QTag", "QType", "QuestionText"]
    if weight_column == None:
        weight_column = "weight"
        freqname = (project_label + "_freq")
    else:
        freqname = (project_label + "_weight_freq")

    print("Uniqueid : ",uniqueid)
    print("SSI responses and question {} için SSIVRDATA'ya aktarılan tablolardan elde edilecek.".format(project_label))

    if query == None:

        print("Data SSIVRDATA'ya aktarılan ({}_datax) tablolardan joinlenerek elde edilecek.".format(project_label))

    else :
        print("Data  yazılan select sorgu ile aktarılacak.SQL sorgusunun doğruluğundan emin olunuz.\n {}".format(query))

    yataydf, df_resp_quest, df = get_Data_Questions_Responses(project_label, uniqueid, query=query)

    df["ColumnValue"] = df["ColumnValue"].astype(str)
    resulttemp = pd.merge(df, df_resp_quest, on=["ColumnName", "ColumnValue"], how="outer")
    resulttemp['weight'] = float('1.0')

    # resulttemp.to_csv(r'C:\apps\resulttemp_{}.csv'.format(project_label), encoding='utf-8-sig',
    #                   header=True, index=True, sep=';', mode='w')

    ###### Data veri tiplerine göre çözümleniyor. ######
    ###### Step1 --> single ###### SelectRadioButton

    dfsingle = resulttemp.loc[resulttemp['QType'].isin(
        ["SelectRadioButton"]) & pd.notnull(resulttemp["FormId"])]

    result.append(dfsingle[selected_columns])

    ###### Step2 --> multi ###### "SelectCheckBox", "SelectComboBox"

    dfmulti = resulttemp.loc[resulttemp['QType'].isin(
        ["SelectCheckBox", "SelectComboBox"]) & pd.notnull(resulttemp["FormId"])]

    dfmulti['ColumnValue'] = dfmulti['ColumnName'].str.split('_').str[1]
    dfmulti['ColumnName'] = dfmulti['ColumnName'].str.split('_').str[0]


    result.append(dfmulti[selected_columns])

    ###### Step3 --> other ###### 'Numeric', 'Text', 'OpenEndMultiLine', 'OpenEndSingleLine'

    dfother = df

    dfother = pd.merge(
        dfother, df_resp_quest[df_resp_quest['QType'].isin(
            ['Numeric', 'Text', 'OpenEndMultiLine', 'OpenEndSingleLine'])]
        [["ColumnName", "ColumnValueText", "QTag", "QuestionText", "QType"]],
        on=["ColumnName"], how="inner")
    
    dfother['weight'] = float('1.0')
    dfother['ColumnValueText'] = dfother['ColumnValue']
    dfother['ColumnValue'] = ''

    result.append(dfother[selected_columns])

    ###### Step4 --> other_SSI ###### SSI startswith sys_%

    dfother2 = df.loc[df['ColumnName'].str.startswith('sys_', na=False)]
    dfother2['ColumnValueText'] = dfother2['ColumnValue']
    dfother2['ColumnValue'] = ''
    dfother2['weight'] = float('1.0')
    dfother2['QTag'] = dfother2['ColumnName']
    dfother2['QType'] = 'SSI_INFO'
    dfother2['QuestionText'] = dfother2['ColumnName']

    result.append(dfother2[selected_columns])

    # Final QmonData

    result = pd.concat(result, axis=0, sort=True)
    result.set_index("FormId", inplace=True)

    result.to_csv(r'C:\apps\{}_QmonData.csv'.format(project_label), encoding='utf-8-sig',
                  header=True, index=True, sep=';', mode='w')

    # Yatay text data oluşturuluyor.
    
    dftext = result.pivot_table(index=["FormId"] , columns='ColumnName' , values='ColumnValueText',aggfunc='first')

    dftext.to_csv(r'C:\apps\{}_YatayTextData.csv'.format(project_label), encoding='utf-8-sig',
                  header=True, index=True, sep=';', mode='w')

    # Frekans 
    
    freq = frekans(result, weight_column)
    
    freq.to_csv(r'C:\apps\{}.csv'.format(freqname), encoding='utf-8-sig',
                  header=True, index=True, sep=';', mode='w')
    
    if cross_filter != None :
        kirilimli_frekans(dftext, freq, cross_filter,df_resp_quest)

run_func(project_label="LSCS1625_4",
         uniqueid = "ORJREF",
         query='select * from LSCS1625_4_data1 where sys_Respstatus=5',
         cross_filter=None)

