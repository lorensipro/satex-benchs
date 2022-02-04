'''
 Parse the original files given in the different contest web site
 to populate a very simple sqlite3 database containing only very
 simple results (solver, bench, time, result)
'''

import sys, os, os.path
import sqlite3
import csv
import yaml, json
import gzip
import logging
import urllib.request, ssl

logging.basicConfig(level=logging.DEBUG)

def pandas_read(filename):
    with gzip.open(filename) as f:
        df = pd.read_csv(filename)
    return df

def handle_2020_parallel(df):
    data = {}
    def makebenchname(b):
        return "2020-contest/" + str(b)
    def makesolvername(s):
        return "2020-contest-parallel:" + str(s)
    
    benchs = [str(x) for x in df["cnf"].unique()]
    solvers = [str(x) for x in df.columns[2:]]
    print(df)
    for i, b in enumerate(benchs):
        assert df.loc[i, "cnf"] == b
        for s in solvers:
            res  = df.loc[0,s]
            if res == "TIMEOUT":
                res = -1
            else:
                res = float(res)
            data[(makebenchname(b), makesolvername(s))] = res

#handle_2020_parallel(pandas_read("raw/2020-contest-parallel-track.csv.gz"))

def get_release_files():
    with urllib.request.urlopen("https://api.github.com/repos/lorensipro/satex-benchs/releases") as up:

        j = json.loads(up.read().decode("utf-8"))
    assets = None
    for release in j:
        if release["tag_name"] == "raw-results":
            assets = release["assets"]
    toret = []
    for a in assets:
        toret.append({"url":a["browser_download_url"], "file":a["name"]})
    
    return toret

def download_or_not(url_base, filename, dd):
    ''' Downloads the remote csv/txt file and put it in the 
    directory "dd" if not already there. Otherwise, do noting '''
    local_file = dd + "/" + filename
    remote_file = url_base

    if not os.path.isdir(dd):
        logging.info("Dir " + dd + "not found, I am creating it...")
        os.mkdir(dd)

    if not os.path.isfile(local_file):
        logging.info("File " + local_file + " not found, I am downloading it...")
        logging.debug("Retrieving " + remote_file)
        ctx = ssl.create_default_context()
        ctx.check_hostname = None
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(remote_file, context = ctx) as remote_stream, \
             open(local_file, "wb") as local_stream:
                local_stream.write(remote_stream.read())


def load_config(filename = "./config.yaml"):
    config = None
    with open("config.yaml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            raise
    return config

config = load_config()
download_dir = config["download_dir"]
files_to_handle = get_release_files()
for fh in files_to_handle:
    url = fh["url"]
    filename = fh["file"]
    download_or_not(url, filename, download_dir)

goodfiles = ['2020-contest-main-track.csv.gz']

for gf in goodfiles:
    with gzip.open("raw/"+gf, 'rt') as f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            good = row['result'] in ['SAT','UNSAT'] and row['status']=='complete' and float(row['time']) < 5000 #and row['verifier-result'].endswith('-VERIFIED')
            print(row['solver'], row['benchmark'], row['result'], float(row['time']) if good else 5000) # if row['status']=='SAT' or row['status']=='UNSAT' else 5000)
