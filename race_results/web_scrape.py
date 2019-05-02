from bs4 import BeautifulSoup
from urllib.request import urlopen
from sqlalchemy import create_engine
from lib.constants import psql, output_path
import pandas as pd
import zlib
import codecs
import os
import psycopg2
import io


class ScrapeTechScoreResults:

    def dev_create_soup_obj(self):
        page_dev = codecs.open(self, 'r', 'utf-8').read()
        soup = BeautifulSoup(page_dev, 'html.parser')
        return soup

    def create_soup_obj(self):
        page = urlopen(self)
        decomp_page = zlib.decompress(page.read(), 16 + zlib.MAX_WBITS)
        soup = BeautifulSoup(decomp_page, 'html.parser')
        return soup

    def extract_raw_results(self):
        school_data_saved = ""
        for record in self.find_all('tr'):
            school_data = ""
            for data in record.find_all('td'):
                school_data = school_data + "," + data.text
            school_data_saved = school_data_saved + "\n" + school_data[1:]
        return school_data_saved

    def raw_results_to_df(self):
        df = pd.DataFrame([x.split(',') for x in self.split('\n')])
        df.columns = ['note', 'finish_pos', 'school', 'division', 'race_1', 'race_2', 'race_3', 'race_4',
                      'race_5', 'blank', 'tot']
        return df

    def save_results_to_csv(self):
        header = "note,finish_pos,school,division,race_1,race_2,race_3,race_4,race_5,blank,tot" + "\n"
        file = open(os.path.expanduser(output_path), "wb")
        file.write(bytes(header, encoding='ascii', errors='ignore'))
        file.write(bytes(self, encoding='ascii', errors='ignore'))

    def write_results_to_db(self):
        engine = create_engine(psql)
        self.head(0).to_sql('raw_results', engine, if_exists='replace', index=False)
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        self.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_from(output, 'raw_results', null="")
        conn.commit()