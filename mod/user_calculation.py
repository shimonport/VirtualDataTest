#!/usr/bin/python3
from typing import List
import pandas as pd
from datetime import datetime
import sf_virtual_data.api.vt_pb2 as vt_pb2
from sf_virtual_data.api.vt_pb2 import FieldObjectMetadata, SiteMetadata, Alarm
import sf_virtual_data.models as models
import math

def debug(res:pd.DataFrame):
    '''
    Write your debugging logic here to be used when script executed with "--debug" cmd line arg,
    you can use it for plotting or anything else
    '''
    pass
def validate1 ( fd: models.FieldData, objects_ids):
    if len(fd) == 5:
        return models.FieldData(data = fd[1:], objects_ids = objects_ids)
    return fd

def validate ( fd: models.FieldData):
    for x in fd:
        if not math.isnan(x):
            return True
    return False

def calculate(start_time_utc: datetime, end_time_utc: datetime, data: pd.DataFrame, site_metadata: SiteMetadata, field_objects_metadata: List[FieldObjectMetadata]) -> pd.DataFrame:
    """
    User main code to calculate data based on input data from solar focus
    """
    res = pd.DataFrame()
    objects_ids = list(map(lambda y: y.id, field_objects_metadata))
    t1 = pd.DataFrame()
    s = pd.Series()
    for index, row in data.iterrows():
        if validate(row["energy"]):
            s[index] = validate1(row["energy"], objects_ids)
    t1["energy"] = s
    data = t1
    df = pd.DataFrame(index=['Daily Energy', 'Total ENergy'])
    energy  = data["energy"]
    energy.index = pd.to_datetime(data.index)
    daily_energy = energy.groupby(energy.index.date)
    start_end_day_energy = pd.concat([daily_energy.head(1), daily_energy.tail(1)]).sort_index()
    daily_grouped = start_end_day_energy.groupby(start_end_day_energy.index.date)
    daily_diff = daily_grouped.diff()
    daily_energy = daily_diff.dropna()
    total_energy = start_end_day_energy.tail(1)


    #df = pd.concat([daily_energy, total_energy], axis=0)
    #df['index'] = ['Daily Energy', 'Total ENergy']
    #print(df)

    total_e = total_energy.explode()
    daily_e = daily_energy.explode()

    inverters = len(total_e)

    if 1 <= inverters:
        df['Inverter 1'] = [str(daily_e.iloc[0]), str(total_e.iloc[0])]
    if 2 <= inverters:
        df['Inverter 2'] = [str(daily_e.iloc[1]), str(total_e.iloc[1])]
    if 3 <= inverters:
        df['Inverter 3'] = [str(daily_e.iloc[2]), str(total_e.iloc[2])]
    if 4 <= inverters:
        df['Inverter 4'] = [str(daily_e.iloc[3]), str(total_e.iloc[3])]


    #TODO: please fill : total
    total: pd.DataFrame = df
    res["energy measurements"] = [total] # ....Energy
    return res


