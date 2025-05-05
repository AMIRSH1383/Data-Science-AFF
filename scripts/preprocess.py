
import pandas as pd
import yaml



CONFIG_PATH = "../config.yml"



def get_adjustment_dict(stocks_list, the_daily_data):
    the_adjustments_dict ={}
    for stock in stocks_list:

        unadj_df = the_daily_data[stock]["raw"]
        adj_df = the_daily_data[stock]["adj"]


        unadj_df['datetime'] = pd.to_datetime(unadj_df["<DTYYYYMMDD>"], format='%Y%m%d')
        adj_df['datetime'] = pd.to_datetime(adj_df["<DTYYYYMMDD>"], format='%Y%m%d')

        merged_df = pd.merge(unadj_df, adj_df, on='datetime', suffixes=('_unadj', '_adj'))
        merged_df['Ratio'] = merged_df['<CLOSE>_adj'] / merged_df['<CLOSE>_unadj']

        final_df = merged_df[['datetime', 'Ratio']]
        the_adjustments_dict[stock] = final_df
    
    return the_adjustments_dict



def apply_adjustments(df_main, df_adj):
    df_main['datetime'] = pd.to_datetime(df_main['datetime'])
    df_adj['date'] = pd.to_datetime(df_adj['date'])
    df_main['date'] = df_main['datetime'].dt.date
    df_adj['date'] = df_adj['date'].dt.date
    df_merged = pd.merge(df_main, df_adj, on='date', how='left')
    for col in ['open', 'high', 'low', 'close']:
        df_merged[col] = df_merged[col] * df_merged['ratio']
    
    df_merged['volume'] = df_merged['volume'] / df_merged['ratio']

    df_adjusted = df_merged.drop(columns=['date', 'ratio'])

    return df_adjusted


def apply_adjustment_on_all_stocks(stocks_list, the_adjustment_dict, the_dataframes):
    for stock in stocks_list:
        the_dataframes[stock] = apply_adjustments(the_dataframes[stock], the_adjustment_dict[stock])




def filter_by_start_date(df, start_date):

    df['date'] = pd.to_datetime(df['date'])
    start_date = pd.to_datetime(start_date)
    return df[df['date'] >= start_date].reset_index(drop=True)



def read_config(path):
    with open(path, 'r') as file:
        config = yaml.safe_load(file)

    stocks = config.get('stocks', [])
    start_date = config.get('start_date', None)
    adjusted = config.get('adjusted', False)

    return stocks, start_date, adjusted

def load_df_from_prev(the_path):
    pd.read_csv(the_path)





def preprocess(stocks_raw_dataframe, daily_stocks_dataframe):
    print(read_config(CONFIG_PATH))

    stocks_list, start_date, should_adjust = read_config(CONFIG_PATH)
    
    final_new_dataframes = stocks_raw_dataframe

    if(start_date!= None):
        final_new_dataframes = filter_by_start_date(final_new_dataframes, start_date)

    if(should_adjust):
        adjustment_dict = get_adjustment_dict(stocks_list, daily_stocks_dataframe)
        final_new_dataframes = apply_adjustment_on_all_stocks(stocks_list, adjustment_dict, final_new_dataframes)

    return final_new_dataframes




