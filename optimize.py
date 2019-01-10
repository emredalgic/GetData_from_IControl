
import pandas as pd

def optimized_df(dfx):
    # referenca link : https://www.dataquest.io/blog/pandas-big-data/
    converted_obj = pd.DataFrame()
    df_obj = dfx.select_dtypes(include=['object']).copy()
    optimized_df = dfx.copy()
    for col in df_obj.columns:
        num_unique_values = len(df_obj[col].unique())
        num_total_values = len(df_obj[col])
    if num_unique_values / num_total_values < 0.5:
        converted_obj.loc[:, col] = df_obj[col].astype('category')
    else:
        converted_obj.loc[:, col] = df_obj[col]
    optimized_df[converted_obj.columns] = converted_obj
    #print(optimized_df.head(10))
    return (optimized_df)
