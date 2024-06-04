#!/usr/bin/python

import sys
import pandas as pd
from pandas import DataFrame
from pandas.io.sql import SQLTable
import logging
import yaml
from urllib.parse import quote
import sqlalchemy as sa
import os
from sqlalchemy import text, Engine, Connection, Table, TextClause
from src.telegram_bot import enviar_mensaje
import asyncio

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')

sql_query_1 = os.path.join(proyect_dir, 'sql', 'df_extend.sql')
sql_query_2 = os.path.join(proyect_dir, 'sql', 'df_headcount.sql')
sql_query_3 = os.path.join(proyect_dir, 'sql', 'df_recording_log.sql')

params = {"interval": 1}

logging.basicConfig(
    level=logging.INFO,
    filename=(os.path.join(proyect_dir, 'log', 'logs_main.log')),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


def get_engine(username: str, password: str, host: str, database: str, port: str, *_) -> Engine:
    return sa.create_engine(f"mysql+pymysql://{username}:{quote(password)}@{host}:{port}/{database}")


with open(os.path.join(proyect_dir, 'config', 'credentials.yml'), 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1, source2, source3, source4 = config['source1'], config['source2'], config['source3'], config['source4']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


def engine_1() -> Connection:
    return get_engine(**source1).connect()


def engine_2() -> Connection:
    return get_engine(**source2).connect()


def engine_3() -> Connection:
    return get_engine(**source3).connect()


def engine_4() -> Connection:
    return get_engine(**source4).connect()


def import_query_date(sql, parameters) -> TextClause:

    with open(sql, 'r') as f_2:

        try:
            querys = f_2.read()
            query = text(querys).bindparams(**parameters)
            return query

        except yaml.YAMLError as e_2:
            logging.error(str(e_2), exc_info=True)


def import_query(sql) -> TextClause:

    with open(sql, 'r') as f_3:

        try:
            querys = f_3.read()
            query = text(querys)
            return query

        except yaml.YAMLError as e_3:
            logging.error(str(e_3), exc_info=True)
        

def extract(query: text, conn: Engine | Connection) -> DataFrame:
    
    with conn as con:
        df = pd.read_sql_query(query, con)
        logging.info(f'se leen {len(df)}')    
        return df
        
        
def to_sql_replace(table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

    satable: Table = table.table
    ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
    data = [dict(zip(ckeys, row)) for row in data_iter]
    values = ', '.join(f':{nm}' for nm in ckeys)
    stmt = f"REPLACE INTO {satable.name} VALUES ({values})"

    con.execute(text(stmt), data)
        
        
def load(name: str, conn: Engine | Connection, action: str, index: bool, df: pd.DataFrame):

    with conn as con:

        try:
            df.to_sql(name=name,con=con, if_exists=action, index=index, method = to_sql_replace)
            logging.info(f'Se cargan {len(df)} datos')
            asyncio.run(enviar_mensaje(f'Se cargan {len(df)} datos'))  
            print(f'Se cargan {len(df)} datos')           

        except KeyError as e_4:

            logging.error(str(e_4), exc_info=True)
            
            
def transform() -> DataFrame:

    df_extend = extract(import_query_date(sql_query_1, params), engine_1())

    df_headcount = extract(import_query(sql_query_2), engine_2())
        
    df_join_exthead = pd.merge(df_extend, df_headcount, left_on='user', right_on='Documento', how='left')
        
    logging.info(f'se cruzan {len(df_join_exthead)}')

    df_join_exthead['gestion'] = df_join_exthead['status'].apply(
                                    lambda x: 'FIDELIZADO' if str(x).startswith('AF') else 'NO-FIDELIZADO')

    df_join_exthead['proceso'] = 'FIDELIZACION-Y-CARTERIZACION'
    df_join_exthead['aliado'] = 'COS-BOGOTA'
    df_join_exthead['segmento'] = 'CONVERGENTE'
    df_join_exthead['subproceso'] = 'FIDELIZACION-HOGAR'

    df_join_exthead['fecha'] = pd.to_datetime(df_join_exthead['call_date'])

    df_join_exthead['day'] = (df_join_exthead['fecha'].dt.day.astype(str)).apply(
                                                    lambda x: '0'+x if 2 > len(x) else x)

    df_join_exthead['month'] = df_join_exthead['fecha'].dt.month.astype(str)

    df_join_exthead['year'] = df_join_exthead['fecha'].dt.year.astype(str)

    df_join_exthead['date'] = df_join_exthead['day']+'-'+df_join_exthead['month']+'-'+df_join_exthead['year']

    df_join_exthead['hora'] = df_join_exthead['call_date'].astype(str).apply(lambda x: x[-8:].replace(':', '-'))

    df_join_exthead['call_date'] = df_join_exthead['call_date'].astype(str)

    df_join_exthead = df_join_exthead.rename(columns={'length_in_sec': 'TMO'})

    df_recording_log = extract(import_query_date(sql_query_3, params), engine_3())
        
    df_recording_log['lead_id1'] = df_recording_log['lead_id1'].astype(str)

    df_join_2 = pd.merge(df_join_exthead, df_recording_log, left_on='lead_id', right_on='lead_id1', how='left').drop(
                                                                                                'lead_id1', axis=1)
        
    df_join_2 = df_join_2.drop(['day', 'month', 'year', 'date'], axis=1)
        
    logging.info(f'se cruzan {len(df_join_2)}')
        
    df_join_2 = df_join_2.dropna(subset=['TMO'])

    df_join_2['TMO'] = df_join_2['TMO'].astype(int)

    df_join_2 = df_join_2.query('TMO > 180')

    df_join_2['tipo_gestion'] = 'outbound'
        
    return df_join_2
