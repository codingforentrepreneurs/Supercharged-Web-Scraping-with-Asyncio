import pathlib
import pandas as pd

from .db import conn as db_conn, verify_table_exists


def df_from_sql(table_name='spoonflower_links'):
    table_exists = verify_table_exists(table_name)
    if not table_exists:
        return pd.DataFrame()
    df = pd.read_sql_table(table_name, db_conn)
    return df

def df_to_sql(df, table_name='spoonflower_links', if_exists='replace'):
    df.to_sql(table_name, db_conn, if_exists=if_exists)
    return df

def store_links_as_df_pickle(datas=[], name='links.pkl'):
    new_df = pd.DataFrame(datas)
    og_df = pd.DataFrame([{'id': 0}])
    if pathlib.Path(name).exists():
        og_df = pd.read_pickle(name) # read_csv
    df = pd.concat([og_df, new_df])
    df.reset_index(inplace=True, drop=False)
    df = df[['id', 'slug', 'path', 'scraped']]
    df = df.loc[~df.id.duplicated(keep='first')]
    # df.set_index('id', inplace=True, drop=True)
    df.dropna(inplace=True)
    df.to_pickle(name)
    return df
