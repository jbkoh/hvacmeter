import pdb
import json
import arrow
import rdflib
import pandas as pd
from functools import reduce
def updater(x, y):
    x.update(y)
    return x

from building_depot import DataService, BDError
from bd3client.CentralService import CentralService as bd3cs
from bd3client.Sensor import Sensor as bd3sensor
from bd3client.Timeseries import Timeseries as bd3ts

PST = 'US/Pacific'
end_time = arrow.get(2018, 4, 15)
begin_time = arrow.get(end_time).shift(days=-10).datetime
end_time = end_time.datetime

def store_data(data, name):
    data = reduce(updater, data['timeseries'], {})
    series = pd.Series(index=data.keys(), data=list(data.values()))
    series.to_csv(name)

def store_bd3_data(data, name):
    data = data['data']['series'][0]['values']
    indices = [arrow.get(datum[0]).datetime for datum in data]
    values = [datum[2] for datum in data]
    series = pd.Series(index=indices, data=values)
    series.to_csv(name)

def load_building_data():

    with open('config/bd2_secret.json', 'r') as fp:
        config = json.load(fp)

    ds = DataService(config['hostname'], config['apikey'], config['user'])

    with open('metadata/ebu3b_bacnet.json', 'r') as fp:
        naes = json.load(fp)

    srcids = []
    for nae_num, nae in naes.items():
        objs = nae['objs'][1:]
        srcids += ['{0}_{1}_{2}'
                   .format(nae_num, obj['props']['type'], obj['props']['instance'])
                   for obj in objs
                   if obj['props']['type'] in [0,1,2,3,4,5,13,14,19]]

    srcid = '506_0_3000485'
    nonexisting_srcids = []
    for srcid in srcids:
        uuid = ds.list_sensors({'source_identifier':srcid})['sensors'][0]['uuid']
        #end_time = arrow.get(arrow.get().datetime, 'US/Pacific').datetime

        try:
            raw_data = ds.get_timeseries_datapoints(uuid, 'PresentValue', begin_time, end_time)
        except:
            print('{0} is not found in ds.'.format(srcid))
            nonexisting_srcids.append(srcid)
            continue
        #data = reduce(updater, raw_data['timeseries'], {})
        #series = pd.Series(index=data.keys(), data=list(data.values()))
        #series.to_csv('./data/{0}.csv'.format(srcid))
        store_data(raw_data, './data/{0}.csv'.format(srcid))


    with open('nonexisting_srcids.json', 'w') as fp:
        json.dump(nonexisting_srcids, fp, indent=2)


def preprocess_ion_metadata():
    with open('metadata/raw_ion_metadata.json', 'r') as fp:
        ions = json.load(fp)

    sensors = {
        'WARREN.EBU3B_BTU_C_H2520_RealTime:HTW ST': 'Hot_Water_Supply_Temperature_Sensor',
        'WARREN.EBU3B_BTU_C_H2520_RealTime:HTW RT': 'Hot_Water_Return_Temperature_Sensor',
        'WARREN.EBU3B_BTU_C_H2520_RealTime:HTW Flo': 'Hot_Water_Flow_Sensor',
        'WARREN.EBU3B_BTU_C_H2520_RealTime:CHW RT': 'Chilled_Water_Return_Temperature_Sensor',
        'WARREN.EBU3B_BTU_C_H2520_RealTime:CHW ST': 'Chilled_Water_Supply_Temperature_Sensor',
        'WARREN.EBU3B_BTU_C_H2520_RealTime:CHW Flo': 'Chilled_Water_Flow_Sensor'
    }
    ion_bd_srcids= {}
    for bd_srcid, metadata in ions.items():
        name = metadata['name']
        if name in sensors:
            srcid = 'ION-' + sensors[name]
            ion_bd_srcids[srcid] = name
    with open('metadata/ebu3b_ion.json', 'w') as fp:
        json.dump(ion_bd_srcids, fp, indent=2)

def load_ion_data():
    with open('config/bd3_ion_secret.json', 'r') as fp:
        config = json.load(fp)
    cs = bd3cs(config['hostname'], config['cid'], config['ckey'])
    sensor_api = bd3sensor(cs)
    ts_api = bd3ts(cs)

    with open('config/bd3_ion_metadata.json', 'r') as fp:
        bd3_ions = json.load(fp)

    for srcid, bd_uuid in bd3_ions.items():
        data = ts_api.getTimeseriesDataPoints(bd_uuid,
                                              arrow.get(begin_time).timestamp,
                                              arrow.get(end_time).timestamp)

        store_bd3_data(data, './data/{0}.csv'.format(srcid))

def load_ion_data_old():
    with open('config/bd2_ion_secret.json', 'r') as fp:
        config = json.load(fp)
    ds = DataService(config['hostname'], config['apikey'], config['user'])
    with open('metadata/ebu3b_ion.json', 'r') as fp:
        ion_srcids = json.load(fp)
    for srcid, bd_srcid in ion_srcids.items():
        uuid = ds.list_sensors({'source_identifier': bd_srcid})['sensors'][0]['uuid']
        raw_data = ds.get_timeseries_datapoints(uuid, 'PresentValue', begin_time, end_time)
        store_data(raw_data, './data/{0}.csv'.format(srcid))

if __name__ == '__main__':
    #load_building_data()
    load_ion_data()
