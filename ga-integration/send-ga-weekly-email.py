import os
import json
import numpy as np
import boto3
import numpy as np
import sqlalchemy
from gahelper import Gahelper
import matplotlib.pyplot as plt
from datetime import datetime
from gaformatter import format_dataframe
from datetime import datetime, timedelta


from gaformatter import *
from gahelper import *

config = json.load(open("../config/config.json"))

db = config['db']
conn_template = 'mysql://{}:{}@{}:{}/{}'
connstr = conn_template.format(db['username'], db['password'], db['host'], db['port'], db['dbname'])
conn = sqlalchemy.create_engine(connstr)

accessKey = config['aws']['accessKeyId']
secretKey = config['aws']['secretAccessKey']
region    = config['aws']['region']
ses = boto3.client('ses', aws_access_key_id=accessKey, aws_secret_access_key=secretKey, region_name=region)

ga = Gahelper(config)

n = datetime.now()
start_date = str(n - timedelta(days=7))[0:10]
end_date = str(n)[0:10]

metrics = ['ga:sessions']
dimensions = ['ga:pagePath']
report = ga.get_report(metrics, dimensions, start_date, end_date)

report['sessions'] = report['sessions'].astype(int)

report.sort_values('sessions', ascending=False, inplace=True)

homepage = report[report['pagePath']=='/'].iloc[0]['sessions']

f = report['pagePath'].apply(lambda x: True if x[0:6]=='/blog/' else False)
blogs = report[f].copy()
blogs.index = np.arange(blogs.shape[0])

blogs['sessions'] = blogs['sessions'] / homepage

blogs['rank'] = blogs.index

blogs.columns = ['pagePath', 'session_share', 'rank']

conn.execute('truncate table popular_blogs')

blogs[0:20].to_sql('popular_blogs', conn, if_exists='append', index=False)

bdy = "<body><b>Most popular blog pages from " + start_date + " to " + end_date + "</b><br/><br/><table border=1>"
bdy += "<tr><td><b>Session Share</b></td><td><b>Post</b></td></tr>"
for r in range(blogs[0:20].shape[0]):
    row = blogs.iloc[r]
    pagePath = row['pagePath']
    ss = row['session_share']
    url = 'https://dataskeptic.com' + pagePath
    i = pagePath.rfind('/') +1
    file = pagePath[i:]
    file = file.replace('-', ' ')
    bdy += "<tr><td>" + str(ss * 100) + "%</td><td>"
    bdy += "<td><a href='" + url + "'>" + file + "</a></td></tr>"

bdy += "</table>"

lst = ['kyle@dataskeptic.com']

CHARSET = "UTF-8"

subject = 'DataSkeptic.com Blog Session Share from ' + start_date + ' to ' + end_date

response = ses.send_email(
    Destination={
        'ToAddresses': lst
    },
    Message={
        'Body': {
            'Html': {
                'Charset': CHARSET,
                'Data': bdy,
            }
        },
        'Subject': {
            'Charset': CHARSET,
            'Data': subject,
        },
    },
    Source='kyle@dataskeptic.com'
)

