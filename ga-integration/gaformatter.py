import numpy as np
import os
from datetime import datetime
import matplotlib.pyplot as plt

def format_dataframe(s3, bucketname, report, metrics, dimensions, start_date, end_date):
    for metric in metrics:
        m = metric[3:]
        print("report is ", report)
        print('metric is ', m)
        print("report[m] is", report[m])
        # report[m] = report[m].astype(int)
    sortby = list(map(lambda x: x[3:], metrics))
    report.sort_values(sortby, ascending=False, inplace=True)
    report.index = np.arange(report.shape[0])

    if len(metrics) == 1 and len(dimensions) == 1:
        m = metrics[0][3:]
        h = dimensions[0][3:]
        print(report)
        
        print('metric is ', m)
        plt.figure()
        plt.barh(report.index, report[m].astype(float))
        plt.gca().yaxis.set_ticks(report.index)
        plt.gca().yaxis.set_ticklabels(report[h])
        fname = m + '_' + h + '_' + start_date + '_' + end_date + '.png'
        plt.savefig(fname)
        dtstr = str(datetime.now())[0:10]
        s3key = 'bot/ga-images/' + dtstr + '/' + fname
        data = open(fname, 'rb')
        s3.Bucket(bucketname).put_object(Key=s3key, Body=data, ACL='public-read')
        os.remove(fname)
        img = "http://dataskeptic-bot.s3.amazonaws.com/" + s3key
        txt = ""
        return {"img": img, "txt": txt}
    else:
        img = ""
        txt = str(report)
        return {"img": img, "txt": txt}
