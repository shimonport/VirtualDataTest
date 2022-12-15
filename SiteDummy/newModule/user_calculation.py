#!/usr/bin/python3
import math
from typing import List
import pandas as pd
from datetime import datetime
import sf_virtual_data.api.vt_pb2 as vt_pb2
from sf_virtual_data.api.vt_pb2 import FieldObjectMetadata, SiteMetadata, Alarm
import sf_virtual_data.models as models

def debug(res:pd.DataFrame):
    '''
    Write your debugging logic here to be used when script executed with "--debug" cmd line arg,
    you can use it for plotting or anything else
    '''
    pass


def sr(fd: models.FieldData, maximum, objects_ids, start):
    for i in range(len(fd)):
        fd[i] = fd[i]/maximum
    return models.FieldData(data = fd, objects_ids = objects_ids[start:])


def get_total_sr(fd: models.FieldData, maximun):
    for i in range(len(fd)):
        fd[i] = fd[i] / maximun
    return fd


def normalize_by_panels(fd: models.FieldData, value, index, panels):
    fd[index] = value/panels
    return fd


def validate ( fd: models.FieldData, objects_ids, start):
    return models.FieldData(data = fd[start:], objects_ids = objects_ids)


def get_sunrise( fd: models.FieldData, start):
    for x in fd[start:]:
        if math.isnan(x) or x == 0:
            return False
    return True


def get_sunset( fd: models.FieldData, start):
    for x in fd[start:]:
        if math.isnan(x) or x == 0:
            return True
    return False
    

def calculate(start_time_utc: datetime, end_time_utc: datetime, data: pd.DataFrame, site_metadata: SiteMetadata, field_objects_metadata: List[FieldObjectMetadata]) -> pd.DataFrame:
    """
    User main code to calculate data based on input data from solar focus
    """
    #print(type(data.iloc[0,0]))
    res = pd.DataFrame()
    start = 0
    total_energy  = data["total_energy"]
    #print(data.iloc[0,0])
    objects_ids = list(map(lambda x: x.id, field_objects_metadata))
    objects_received = len(data.iloc[0,0])
    meta_objects = len(objects_ids)
    if objects_received > meta_objects:
        start = 1

    clean_data = pd.DataFrame()
    dates = list(pd.Series(data.index.date).drop_duplicates())
    for date in dates:
        tmp = pd.DataFrame()
        tmp_series = pd.Series()
        sub_data = data[data.index.date == date]
        sunrise = None
        sunset = None
        prev = None
        light = False
        
        for index, row in sub_data.iterrows():
            tmp_series[index] = validate(row["total_energy"], objects_ids, start)

            if get_sunset(row["total_energy"], start) and light:
                sunset = prev

            if get_sunrise(row["total_energy"], start):
                if not sunrise:
                    sunrise = index
                light = True
            else:
                light = False

            prev = index
        #start_time = datetime.strftime(start_time,'%Y-%m-%d %H:%M:%S')
        #end_time = datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S')
        #print(tmp_series)
        tmp["total_energy"] = tmp_series
        sub_data = tmp[sunrise:sunset]
        clean_data = pd.concat([clean_data, sub_data])

    dates_data = clean_data.groupby(clean_data.index.date)
    date_edges = pd.concat([dates_data.head(1), dates_data.tail(1)]).sort_index()
    daily_energy = (date_edges.diff()).groupby(date_edges.index.date).tail(1)
    #daily_energy = date_diff.dropna()

    # normalize energy dividing by panels number
    normalized_energy = pd.DataFrame()
    # loop through inverters
    for i in range(meta_objects):
        panels = None
        if i == 0:
            panels = 112
        if i == 1:
            panels = 62
        if i == 2:
            panels = 50
        if i == 3:
            panels = 101

        if i+1 in objects_ids:
            normalized_energy["total_energy"] = daily_energy["total_energy"].apply(lambda x: normalize_by_panels(x, x[i], i, panels))

    total_sr = normalized_energy.sum()
    total_sr.index = normalized_energy.tail(1).index
    daily_max_yield = normalized_energy["total_energy"].apply(lambda y: max(y))

    tmp_series = pd.Series()
    inverters_sr = pd.DataFrame()
    i = 0
    for index, row in normalized_energy.iterrows():
        tmp_series[index] = sr(row["total_energy"], daily_max_yield.iloc[i], objects_ids, start)
        i += 1

    inverters_sr["sr"] = tmp_series
    total_sr = total_sr.apply(lambda x: get_total_sr(x, max(x)))

    print(total_sr)
    print(inverters_sr)

    #Fsr = sum(inverters_sr)/len(objects_ids) to get the SR of the site sum all res and devide by inverters
    #print(Fsr)

    # LEFT TO ADD A THRESHOLD FOR UNDERPERFORMANCE
    res["daily_sr"] = inverters_sr # Series content type should be models.FieldData
    res["total_sr"] = total_sr
    return res
