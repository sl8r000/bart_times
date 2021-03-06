import os
import requests
import xmltodict
import dateutil
import collections
import pandas as pd
import time, datetime
import traceback
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.ERROR)

STATIONS = ['ROCK', 'MCAR', '19TH', '12TH', 'WOAK', 'EMBR', 'MONT', 'POWL', 'CIVC']

class Store(object):
    def send(self):
        raise NotImplementedError()

    def get(self):
        raise NotImplementedError()

class DFStore(Store):
    def __init__(self, filename=None):
        self.df = None
        self.filename = filename

    def send(self, new_data):
        new_df = pd.DataFrame(new_data, index=[0])
        if self.df is None:
            self.df = new_df
        else:
            self.df = self.df.append(new_df, ignore_index=True)

        if self.filename is not None:
            with open(self.filename, 'w') as stream:
                self.df.to_csv(stream)

    def get(self):
        return self.df

class CSVStore(Store):
    def __init__(self, filename):
        self.filename = filename
        self.firstwrite = True

    def send(self, new_data):
        if self.firstwrite:
            with open(self.filename, 'w') as stream:
                stream.write(",".join([str(x) for x in new_data.keys()]) + '\n')
            self.firstwrite = False

        with open(self.filename, 'a') as stream:
            stream.write(",".join([str(x) for x in new_data.values()]) + '\n')

    def get(self):
        df = pd.read_csv(self.filename)
        df.time = pd.to_datetime(df.time)
        return df

class Scraper(object):
    def __init__(self, api_key=None, stores=None):
        if api_key is None:
            api_key = os.environ['BART_API_KEY']
        self.api_key = api_key
        self.url = 'http://api.bart.gov/api/etd.aspx?key={api_key}&cmd=etd&orig=all'.format(api_key=self.api_key)

        self.last_fetch = None
        self.stores = stores

    def fetch(self):
        output = dict()
        resp = requests.get(self.url)

        try:
            raw = xmltodict.parse(resp.text)
            output['time'] = dateutil.parser.parse(raw['root']['date'] + ' ' + raw['root']['time'])

            for station in raw['root']['station']:
                if station['abbr'] in STATIONS:
                    for line in station['etd']:
                        if line['destination'] in ['SFO/Millbrae', 'SF Airport', 'Daly City', 'Millbrae']:
                            estimates = line['estimate']
                            if type(estimates) != list:
                                estimates = [estimates]
                            for index, estimate in enumerate(estimates):
                                try:
                                    if estimate['color'] == 'YELLOW':
                                        if estimate['minutes'] == 'Leaving':
                                            minutes = 0
                                        else:
                                            minutes = int(estimate['minutes'])
                                        output[station['abbr'] + '_' + str(index)] = minutes
                                except Exception:
                                    with open('_dumb_store.txt', 'a') as stream:
                                        stream.write(resp.text)
                                        stream.write('\n')
        except Exception as e:
            print resp.url
            logging.error('Fetch failed.\n\t Response: {resp} \n\t Error: {error}'.format(resp=resp.text, error=e.message))

        sorted_output = collections.OrderedDict(sorted(output.items()))
        self.last_fetch = sorted_output
        return sorted_output

    def sync(self, data=None):
        if data is None:
            data = self.last_fetch
        for store in self.stores:
            store.send(data)

if __name__ == '__main__':
    dfs = DFStore('df_backup.csv')
    csvs = CSVStore('data.csv')
    scraper = Scraper(api_key='MW9S-E7SL-26DU-VV8V', stores=[dfs, csvs])

    while True:
        start_time = time.time()
        try:
            x = scraper.fetch()
            scraper.sync()
        except Exception as e:
            logging.error(traceback.format_exc())
        end_time = time.time()
        duration = end_time - start_time
        time.sleep(max([0, 30 - duration]))
