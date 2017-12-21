import json
import paypalrestsdk
import sqlalchemy
from paypalrestsdk import Payment
import logging
import os
import pandas as pd
import sys

logname = sys.argv[0]
logger = logging.getLogger(logname)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logger.setLevel(logging.INFO)

logfile = '/var/tmp/' + logname + '.log'
ldir = os.path.dirname(logfile)
if not(os.path.isdir(ldir)):
    os.makedirs(ldir)
hdlr = logging.FileHandler(logfile)

hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
stdout = logging.StreamHandler()
logger.addHandler(stdout)



def get_start_time(conn):
    query = "SELECT max(create_time) as create_time FROM paypal_payments"
    df = pd.read_sql(query, conn)
    ct = df.iloc[0]['create_time']
    if ct is None:
        return "2016-01-01T00:00:00Z"
    else:
        return ct.isoformat() + 'Z'

def get_payments_since(next_id=None, conn=None):
    if next_id is None:
        start_time = get_start_time(conn)
        payment_history = Payment.all({"count": 20, "start_time": start_time})
    else:
        payment_history = Payment.all({"count": 20, "start_id": next_id})
    """
    p1 = payment_history.to_dict()
    p2 = ph2.to_dict()
    print([p1['next_id'], p2['next_id']])
    print(p1['payments'][0]['create_time'])
    print(p1['payments'][19]['create_time'])
    print(p2['payments'][0]['create_time'])
    print(p2['payments'][19]['create_time'])
    """
    rows = []
    for payment in payment_history.payments:
        p = payment.to_dict()
        id = p['id']
        intent = p['intent']
        state = p['state']
        create_time = p['create_time']
        payment_method = p['payer']['payment_method']
        payer_status = p['payer']['status']
        phone = p['payer']['payer_info']['phone']
        first_name = p['payer']['payer_info']['first_name']
        email = p['payer']['payer_info']['email']
        country = p['payer']['payer_info']['country_code']
        lastname = p['payer']['payer_info']['last_name']
        payer_id = p['payer']['payer_info']['payer_id']
        sa = p['payer']['payer_info']['shipping_address']
        address_line1 = sa['line1']
        shipping_name = sa['recipient_name']
        shipping_city = sa['city']
        shipping_postal_code = sa['postal_code']
        shipping_state = sa['state']
        shipping_country = sa['country_code']
        num_transactions = len(p['transactions'])
        t = p['transactions'][0]
        desc = t['description']
        total = float(t['amount']['total'])
        currency = t['amount']['currency']
        fee = float(t['related_resources'][0]['sale']['transaction_fee']['value'])
        row = {
            "id": id,
            "intent": intent,
            "state": state,
            "create_time": create_time,
            "payment_method": payment_method,
            "payer_status": payer_status,
            "phone": phone,
            "first_name": first_name,
            "email": email,
            "country": country,
            "lastname": lastname,
            "payer_id": payer_id,
            "address_line1": address_line1,
            "shipping_name": shipping_name,
            "shipping_city": shipping_city,
            "shipping_postal_code": shipping_postal_code,
            "shipping_state": shipping_state,
            "shipping_country": shipping_country,
            "num_transactions": num_transactions,
            "desc": desc,
            "total": total,
            "currency": currency,
            "fee": fee
        }
        rows.append(row)
    if len(rows):
        return None
    df = pd.DataFrame(rows)
    try:
        next_id = payment_history['next_id']
    except KeyError:
        next_id = None
    return df, next_id

def save_payments(payments, conn):
    print(payments.columns)
    payments['description'] = payments['desc']
    del payments['desc']
    payments['paypal_id'] = payments['id']
    del payments['id']
    payments.to_sql("paypal_payments", conn, if_exists="append", index=False)

if __name__ == '__main__':
    config = json.load(open('../config/config.json', 'r'))
    api = paypalrestsdk.configure({
      "mode": "live", # sandbox or live
      "client_id": config['paypal']['client_id'],
      "client_secret": config['paypal']['client_secret']
      })
    db = config['db']
    conn_template = 'mysql://{}:{}@{}:{}/{}'
    connstr = conn_template.format(db['username'], db['password'], db['host'], db['port'], db['dbname'])
    conn = sqlalchemy.create_engine(connstr)
    allpayments = []
    payments, next_id = get_payments_since(conn=conn)
    allpayments.append(payments)
    while next_id is not None:
        print(next_id)
        payments, next_id = get_payments_since(next_id=next_id)
        allpayments.append(payments)
    logger.info("New payments to record: " + str(len(allpayments)))
    df = pd.concat(allpayments)
    if df.shape[0] > 0:
        logger.info("Saving")
        save_payments(df, conn)
    logger.info("Done")

    