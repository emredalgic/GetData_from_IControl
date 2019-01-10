#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sql_connection import SqlConnection
from optimize import optimized_df

conn = SqlConnection()


def getstandartdata(tablename):
    """
    Definition : getstandartdata(tablename) \n
    Returns : dataframe \n
    Run function : getstandartdata('LSCS1378_49') \n
    Comments : Icontrolden aktarılan standart dataları data sayısını belirleyip tek data bir dataya append eder. \n
    """
    df = pd.DataFrame
    tablo_adet = conn.sql_readtable("""select count(1) from sys.tables where name like '{}_data%'  AND 
                               ISNUMERIC(right(name,1))=1 """.format(tablename)).values[0].astype('int')
    print(tablename," -->",tablo_adet,"adet tablo var.")
    
    if tablo_adet == 1 :
        df = conn.sql_readtable(
            """ select * from %s_data1 where left(sys_Respstatus,1) = 5 """ % (tablename))
    elif tablo_adet > 1:
        appended_data = []
        for t in range(1,tablo_adet[0] + 1 ):
            if t == 1 :
                data = conn.sql_readtable(
                    """ select * from %s_data%d  where left(sys_Respstatus,1) = 5 """ % (tablename, t), "sys_RespNum")
            else :
                data = conn.sql_readtable(
                    """ select * from %s_data%d  """ % (tablename, t), "sys_RespNum")
            data = optimized_df(data)
            appended_data.append(data)
            print("Data table :",t)
        appended_data = pd.concat(appended_data, axis=1 ,join="inner" )
        df = appended_data
    else :
        raise ValueError("Veritabanında {} adlı tablo bulunamadı !".format(tablename))
    return(df)


def getdata(query):
    """
    Definition : getdata(tablename,uniqueid) \n
    Returns : dataframe  \n
    """
    df = pd.DataFrame
    df = conn.sql_readtable(query)

    return(df)


def get_Data_Questions_Responses(project_label, uniqueid, query=None):
    # SSIVRDATA dan data aliniyor.
    if query == None:
        yataydf = getstandartdata(project_label)
    else:
        yataydf = getdata(query)
    
    # SSI responses and questions aliniyor.
    df_resp_quest = conn.sql_readtable(
        """
        select  quest.ref
        --,quest.ProjectId
        -- ,QOrder
        ,case when QType in ('SelectComboBox','SelectCheckBox') then  quest.QTag + '_' + convert(varchar(1000),Qval) else quest.QTag End  ColumnName
        ,convert(varchar(100),case when QType in ('SelectComboBox','SelectCheckBox') then '1' else QVal  end)  ColumnValue 
        ,[QText] ColumnValueText 
        ,quest.QTag QTag
        ,QuestionText 
        ,QType
        from [dbo].[SSI_Questions] quest 
        left join [dbo].[SSI_Responses] resp 
        on quest.ProjectId = resp.ProjectId and quest.QTag = resp.QTag 
        where quest.ProjectId ='{}'
        """.format(project_label), "ref")

    # Data devriliyor.
    print("Data devriliyor.")
    df = pd.melt(yataydf, id_vars=uniqueid, var_name="ColumnName",
                 value_name="ColumnValue")

    df = df.loc[pd.notnull(df["ColumnValue"])]
    df = optimized_df(df)
    df.rename(index=str, columns={uniqueid: "FormId"}, inplace=True)
    # df_resp_quest.to_csv(r'C:\apps\{}.csv'.format("df_resp_quest"), encoding='utf-8-sig',
    #           header=True, index=True, sep=';', mode='w')
    
    return(yataydf, df_resp_quest, df)
