#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from optimize import optimized_df

def frekans(dframe, weight_column):
    dframe = optimized_df(dframe)

    resultfreq = dframe.groupby(
        ["ColumnName", "QuestionText","QType", "ColumnValue", "ColumnValueText"])[(weight_column)].agg(["sum"]).reset_index()
    
    resultfreq.rename(index=str, columns={"sum": "count"}, inplace=True)

    dframe['distinctcount'] = dframe.index
    resultfreq2 = dframe.groupby(
        ["ColumnName"])["distinctcount"].agg(['nunique']).reset_index()
    
    resultfreq = pd.merge(
        resultfreq, resultfreq2, how="outer", on=["ColumnName"])

    resultfreq['county'] = round(
        resultfreq["count"] / resultfreq["nunique"], 4)

    # resultfreq["ColumnValue"] = resultfreq["ColumnValue"].astype(np.int64)
    resultfreq = resultfreq.sort_values(
        ["ColumnName", "QuestionText", "ColumnValue"])
    
    resultfreq.drop(['nunique'], axis=1, inplace=True)


    return(resultfreq)


