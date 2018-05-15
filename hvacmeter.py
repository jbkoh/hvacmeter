import pdb
import sys
from functools import reduce

import matplotlib.pyplot as plt
import arrow
import pandas as pd
from pandas.errors import EmptyDataError
import numpy as np
from scipy import interpolate
import statsmodels.api as sm

from sparqlwrapper_brick import BrickEndpoint

series_checker = lambda x: isinstance(x, pd.Series)

class HvacMeter(object):
    """
    OneVAV-Points model
    {
        "vav": {
            "srcid": "XXX",
            "room" :"RRR,
            "cc": "YYY",
            "saf": "ZZZ",
            "znt": "KKK"
            }
    }

    OneAHU-VAV model
    {
        "ahu": {
            "vavs": ["vav1", "vav2", ...],
            "mixt": "AAA",
            "supt": "BBB",
            }
            }

    """
    def __init__(self, target_building, brick_endpoint):
        self.brick = brick_endpoint
        self.target_building = target_building
        #ttlfile = '../../../repo/hvacmeter/metadata/ebu3b_brick.ttl'
        ttlfile = '/home/jbkoh/repo/hvacmeter/metadata/ebu3b_brick.ttl'
        self.brick.load_ttlfile(ttlfile)
        self.begin_time = arrow.get(2018, 4, 6).datetime
        self.end_time = arrow.get(2018, 4, 14).datetime
        self.init_df()
        self.datadir = './data/'

    def init_df(self):
        self.datetimes = pd.date_range(self.begin_time,
                                        self.end_time,
                                        freq='5min')
        self.df = pd.DataFrame(index=self.datetimes)
        self.base_ts = [arrow.get(dt).timestamp for dt in self.datetimes]
        self._init_model_params()

    def _init_model_params(self):
        self._init_cooling_params()

    def _init_cooling_params():
        self.df['Q_ahu_cooling_power'] = None
        self.df['Q_vav_cooling_power'] = None
        self.df['Q_ahu_returned_power'] = None
        self.df['water_thermal_power'] = None
        self.df['C3'] = 1

    def get_ahus(self):
        qstr = 'select ?ahu where {?ahu a/rdf:subClassOf* brick:AHU.}'
        self.ahus = [row[0] for row in self.brick.query(qstr)[1]]
        return self.ahus

    def get_ahu_points(self, ahu):
        qstr = """
        select ?oat ?mat ?rat ?dat where {{
            OPTIONAL {{
                ?oat a brick:Outside_Air_Temperature_Sensor .
                ?oat bf:isPointOf <{0}>.
            }}
            OPTIONAL {{
                ?mat a brick:Mixed_Air_Temperature_Sensor .
                ?mat bf:isPointOf <{0}>.
            }}
            OPTIONAL {{
                ?rat a brick:Return_Air_Temperature_Sensor .
                ?rat bf:isPointOf <{0}>.
            }}
            OPTIONAL {{
                ?dat a brick:Discharge_Air_Temperature_Setpoint.
                ?dat bf:isPointOf <{0}>.
            }}
        }}
        """.format(ahu)
        res = self.brick.query(qstr)
        points = {varname: entity for varname, entity
                  in zip(res[0], res[1][0])}
        return points

    def get_ahu_vavs(self, ahu):
        """ This is a backup note.
            BIND(
                IF(
                    NOT EXISTS{{
                        ?sat bf:isPointOf ?vav .
                        ?sat a brick:Supply_Air_Temperature_Sensor .
                        }}
                    , ?sat, ?dat)
                AS ?dat
            )
        """
        qstr = """
        select ?vav ?zone ?znt ?saf ?dat ?sat where {{
            <{0}> bf:feeds+ ?vav .
            ?vav a brick:VAV .
            ?vav bf:feeds+ ?zone .
            ?zone a brick:HVAC_Zone .
            ?znt bf:isPointOf ?vav .
            ?znt a brick:Zone_Temperature_Sensor .
            ?saf bf:isPointOf ?vav .
            ?saf a brick:Supply_Air_Flow_Sensor .

            ?dat bf:isPointOf <{0}>.
            ?dat a brick:Discharge_Air_Temperature_Setpoint .
            OPTIONAL{{
            ?sat a brick:Supply_Air_Temperature_Sensor .
            ?sat bf:isPointOf ?vav .
            }}
            #BIND(IF(exists{{
            #    ?sat a brick:Supply_Air_Temperature_Sensor .
            #    ?sat bf:isPointOf ?vav .
            #    }}, "yes", ?dat_cand) AS ?dat
            #    #}}, ?sat, ?dat_cand) AS ?dat
            #)

        }}
        """.format(ahu)
        res = self.brick.query(qstr)

    def get_ahu_disch_airflow(self, ahu):
        qstr = """
        select ?daf where {{
            ?daf bf:isPointOf <{0}>.
            ?daf a brick:Discharge_Air_Flow_Sensor .
        }}
        """.format(ahu)
        res = self.brick.query(qstr)
        if res[1]: # If DAF exists for the AHU.
            airflow = None # TODO
        else: # If AHU's DAF does not exist, collect VAVs' SAF
            airflow = self.calc_tot_vavs_airflow(ahu)
        return airflow

    def calc_tot_vavs_airflow(self, ahu):
        qstr = """
        select ?saf where {{
            <{0}> bf:feeds ?vav .
            ?vav a brick:VAV.
            ?saf bf:isPointOf ?vav.
            ?saf a brick:Supply_Air_Flow_Sensor .
        }}
        """.format(ahu)
        [var_names, tuples] = self.brick.query(qstr)
        safs = [tup[0] for tup in tuples]
        saf_values = [self.get_point_data(saf) for saf in safs]
        saf_sum = sum([saf_value for saf_value in saf_values
                       if isinstance(saf_value, pd.Series)])
        return saf_sum

    def calc_ahu_returned_power(self, ahu):
        daf = self.get_ahu_disch_airflow(ahu)
        ahu_points = self.get_ahu_points(ahu)
        rat = self.get_point_data(ahu_points['?rat'])
        mat = self.get_point_data(ahu_points['?mat'])
        power = daf.multiply(rat - mat)
        self.df['Q_ahu_returned_power'] = power

    def calc_ahu_cooling_power(self, ahu):
        daf = self.get_ahu_disch_airflow(ahu)
        ahu_points = self.get_ahu_points(ahu)
        dat = self.get_point_data(ahu_points['?dat'])
        mat = self.get_point_data(ahu_points['?mat'])
        power = daf.multiply(mat - dat)
        self.df['Q_ahu_cooling_power'] = power
        
    def calc_vavs_cooling_power(self, ahu): #TODO: Test if this is working.
        point_sets = self.get_vavs_points(ahu)
        powers = [self.calc_vav_cooling_power(points) for points in point_sets.values()]
        none_cnt = sum([not isinstance(power, pd.Series) for power in powers])
        powers_sum = sum([power for power in powers if isinstance(power, pd.Series)]) * len(powers) / (len(powers) - none_cnt)
        self.df['Q_vav_cooling_power'] = powers_sum
    
    def calc_vav_cooling_power(self, vav_points):
        if not vav_points:
            return None
        znts = self.get_point_data(vav_points['?znt'])
        dats = self.get_point_data(vav_points['?dat'])
        safs = self.get_point_data(vav_points['?saf'])
        if False not in map(series_checker, [znts, dats, safs]):
            res = safs.multiply(znts-dats)
            return res
        else:
            return None

    def get_vavs(self, ahu):
        qstr = """
        select ?vav where {{
            <{0}> bf:feeds+ ?vav.
            ?vav a brick:VAV.
        }}
        """.format(ahu)
        res = self.brick.query(qstr)
        vavs = [row[0] for row in res[1]]
        return vavs
        
    def get_vavs_points(self, ahu):
        qstr = """
        select ?vav ?znt ?saf ?dat ?sat ?zone where {{
            ?dat bf:isPointOf <{0}>.
            ?dat a brick:Discharge_Air_Temperature_Setpoint .
            
            <{0}> bf:feeds ?vav .
            ?vav a brick:VAV .
            
            ?vav bf:feeds ?zone .
            ?zone a brick:HVAC_Zone .

            ?znt bf:isPointOf ?vav .
            ?znt a brick:Zone_Temperature_Sensor .

            ?saf bf:isPointOf ?vav .
            ?saf a brick:Supply_Air_Flow_Sensor .

            OPTIONAL{{
                ?sat a brick:Supply_Air_Temperature_Sensor .
                ?sat bf:isPointOf ?vav .
            }}
        }}
        """.format(ahu)
        res = self.brick.query(qstr)
        var_names = res[0]
        point_sets = {} # key: vav, value: points
        for row in res[1]:
            points = {
                '?znt': row[var_names.index('?znt')],
                '?saf': row[var_names.index('?saf')],
                '?dat': row[var_names.index('?sat')] if row[var_names.index('?sat')] else \
                        row[var_names.index('?dat')],
                '?zone': row[var_names.index('?zone')]
            }
            vav = row[var_names.index('?vav')]
            if vav in point_sets:
                print('VAV should not occur twise')
            point_sets[vav] = points
        return point_sets

    def get_vav_points(self, vav):
        qstr = """
        select ?znt ?saf ?dat ?sat ?zone where {{
            <{0}> bf:feeds ?zone .
            ?zone a brick:HVAC_Zone .

            ?znt bf:isPointOf <{0}> .
            ?znt a brick:Zone_Temperature_Sensor .

            ?saf bf:isPointOf <{0}> .
            ?saf a brick:Supply_Air_Flow_Sensor .

            ?ahu bf:feeds <{0}>.
            ?dat bf:isPointOf ?ahu.
            ?dat a brick:Discharge_Air_Temperature_Setpoint .

            OPTIONAL{{
                ?sat a brick:Supply_Air_Temperature_Sensor .
                ?sat bf:isPointOf <{0}> .
            }}
        }}
        """.format(vav)
        res = self.brick.query(qstr)
        var_names = res[0]
        rows = res[1]
        if not rows:
            return None
        row = res[1][0]
        points = {
            '?znt': row[var_names.index('?znt')],
            '?saf': row[var_names.index('?saf')],
            '?dat': row[var_names.index('?sat')] if row[var_names.index('?sat')] else \
                    row[var_names.index('?dat')],
            '?zone': row[var_names.index('?zone')]
        }
        return points

    def get_point_data(self, point, aligned=True):
        qstr = """
        select ?srcid where {{
            <{0}> bf:srcid ?srcid.
        }}
        """.format(point)
        res = self.brick.query(qstr)
        srcid = res[1][0][0]
        try:
            data = pd.Series.from_csv(self.datadir + '{0}.csv'.format(srcid))
        except EmptyDataError:
            return None
        except Exception as e:
            pdb.set_trace()
            print(e)
            sys.exit()
        ts = [arrow.get(dt).timestamp for dt in data.keys()]
        if aligned:
            res = np.interp(self.base_ts, ts, data.values)
            data = pd.Series(data=res, index=[arrow.get(t) for t in self.base_ts])
        return data

    def get_chilled_water_sensors(self):
        qstr = """
        select ?cwf ?cwst ?cwrt where {
            ?cwf a brick:Chilled_Water_Flow_Sensor.
            ?cwf bf:srcid ?cwf_srcid.
            FILTER(CONTAINS(?cwf_srcid, "ION"))

            ?cwst a brick:Chilled_Water_Supply_Temperature_Sensor .
            ?cwrt a brick:Chilled_Water_Return_Temperature_Sensor .
        }
        """
        [varnames, rows] = self.brick.query(qstr)
        points = {varname: value for varname, value in zip(varnames, rows[0])}
        return points

    def calc_chilled_water_usage(self):
        points = self.get_chilled_water_sensors()
        cwrt = self.get_point_data(points['?cwrt'])
        cwst = self.get_point_data(points['?cwst'])
        cwf = self.get_point_data(points['?cwf'])
        self.df['water_thermal_power'] = cwf.multiply(cwrt - cwst)

    def fit_coefficients(self):
        x = self.df[['Q_vav_cooling_power', 'Q_ahu_returned_power', 'C3']]
        y = self.df['water_thermal_power']
        self.model = sm.OLS(y, x).fit()

if __name__ == '__main__':
    brick_endpoint = BrickEndpoint('http://localhost:8890/sparql', '1.0.2')
    hvacmeter = HvacMeter('ebu3b', brick_endpoint)
    hvacmeter.calc_chilled_water_usage()
    ahus = hvacmeter.get_ahus()
    ahu = ahus[0]
    #hvacmeter.calc_ahu_cooling_power(ahu)
    hvacmeter.calc_ahu_returned_power(ahu)
    vavs = hvacmeter.get_vavs(ahu)
    hvacmeter.calc_vavs_cooling_power(ahu)
    hvacmeter.fit_coefficients()
