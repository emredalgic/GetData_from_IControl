#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from optimize import optimized_df


def kirilimli_frekans(yataydf, freqdf, cross_filter,df_resp_quest):
    # worksheet = writer.sheets["sheet1"]
    result = []  # append edilecek boş bir liste yaratılıyor.
    resultg = []
    singlekolon = []
    for i in df_resp_quest["ColumnName"].unique():
        j = df_resp_quest[df_resp_quest["ColumnName"] == i]["QType"].unique()[0]

        if j in ["SelectRadioButton"] and i in yataydf.columns  :
            print(i, j)
            yataydf.columns = yataydf.columns.astype(str)
            
            fgabsolut = pd.crosstab(
                yataydf[i], yataydf[cross_filter], margins=True, margins_name="Toplam")
            fgpercentages = pd.crosstab(yataydf[i], yataydf[cross_filter])
            print(fgabsolut)
            absolut_counter = int(fgabsolut.shape[0]+3)
            percetages_counter = int(fgpercentages.shape[0]+3)
            print(absolut_counter, percetages_counter)
            print(fgpercentages.apply(lambda r: round(r/r.sum(), 3), axis=0))
            singlekolon.append(i)

            result.append(fgabsolut)
            resultg.append(fgpercentages.apply(lambda r: round(r/r.sum(), 3), axis=0))

        elif j in ["SelectCheckBox", "SelectComboBox"] and i in yataydf.columns  :
            pass
            # pivot ile multi responselar value adedi kadar açılıp mp için kırılım verilebilir ?
    
    fn = pd.concat(result, axis=0, sort=True,keys=singlekolon)
    fng = pd.concat(resultg, axis=0, sort=True,keys=singlekolon)
    # print(fn)
    fn.to_csv(r'C:\apps\{}.csv'.format("fn"), encoding='utf-8-sig',
                  header=True, index=True, sep=';', mode='w')
    fng.to_csv(r'C:\apps\{}.csv'.format("fng"), encoding='utf-8-sig',
                  header=True, index=True, sep=';', mode='w')
    
    return(yataydf)
