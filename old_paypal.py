import json
import paypalrestsdk
import pandas as pd
import sqlalchemy

def get_history(api, e):
    payment_history = paypalrestsdk.Payment.all({"count": 100})
    subscriptions = []
    for payment in payment_history['payments']:
        email = payment['payer']['payer_info']['email']
        customer_id = email
        amt = float(payment['transactions'][0]['amount']['total'])
        created = payment['create_time']
        quantity = 1
        plan = ''
        start = payment['create_time']
        status = payment['state']
        id = payment['id']
        src = 'paypal'
        subscription = {"email": email, "amt": amt, "created": created, "quantity": quantity, "plan": plan, "start": start, "status": status, "id": id, "src": src}
        subscriptions.append(subscription)
        adr = payment['payer']['payer_info']['shipping_address']
        line1 = adr['line1']
        if 'address_line2' in adr:
            line2 = adr['line2']
        else:
            line2 = ''
        city = adr['city']
        state = adr['state']
        postal = adr['postal_code']
        brand = ''
        country = adr['country_code']
        funding = ''
        last4 = ''
        tpl = "INSERT INTO stripe_customers (customer_id, line1, line2, city, state, postal, country, brand, funding, last4) VALUES ('{customer_id}', '{line1}', '{line2}', '{city}', '{state}', '{postal}', '{country}', '{brand}', '{funding}', '{last4}')"
        q = tpl.format(customer_id=customer_id, line1=line1, line2=line2, city=city, state=state, postal=postal, country=country, brand=brand, funding=funding, last4=last4)
        e.execute(q)
    df = pd.DataFrame(subscriptions)
    df.to_sql('subscribers', e, if_exists='append', index=False)


if __name__ == '__main__':
    config = json.load(open('../config/config.json', 'r'))
    db = config['db']
    conn_template = 'mysql+pymysql://{}:{}@{}:{}/{}'
    connstr = conn_template.format(db['username'], db['password'], db['host'], db['port'], db['dbname'])
    e = sqlalchemy.create_engine(connstr)
    client_id = config['paypal']['client_id']
    client_secret = config['paypal']['client_secret']
    api = paypalrestsdk.configure({
      "mode": "live", # sandbox or live
      "client_id": client_id,
      "client_secret": client_secret })
    get_history(api, e)
