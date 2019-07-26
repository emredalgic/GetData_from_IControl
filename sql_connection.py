#!/usr/bin/env python
# coding: utf-8

import operator
import pymssql
import pandas as pd
from pandas.io import sql

class SqlConnection :
    """
    Class SqlConnection mssql
    """

    def __init__(self, servername='Emeatristsql', databasename='database') :
        # Connection string / windows authentication
        self.databasename = databasename
        self.servername = servername
        self.data = pd.DataFrame()
        self.connection = pymssql.connect(
            server=self.servername, database=self.databasename)
        # self.connection.close()

    def table_control(self,table_name):
        self.table_name = table_name
        script = ("select COUNT(1) kolon_adet from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{}'".format(table_name))
        dframe = pd.read_sql(script, self.connection)
        return (int(dframe.values[0]))

    def sql_readtable(self,script, index=None):
        """
        Definition : sql_execution(script,index=None)
        Type : Sql execution from mssql.
        Returns : Dataframe

        Run function : sql_execution("select * from %s_data1 where left(sys_Respstatus,1) = 5 "%(tablename))

        """
        self.script = script

        if index == None:
            dframe = pd.read_sql(self.script, self.connection)
        else:
            dframe = pd.read_sql(self.script, self.connection, index_col=index)
        
        return(dframe)
    
    def sql_execution(self,script):
        self.script = script
        sql.execute(self.script, self.connection)
        self.connection.commit()

