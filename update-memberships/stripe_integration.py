import json
import stripe
import sqlalchemy
import pandas as pd


def update_stripe_customers(e, df):
    rows = []
    for r in range(df.shape[0]):
        row = df.iloc[r].to_dict()
        customer_id = row['customer_id']
        #customer_id = 'cus_9p0Ut0lk3NqAwf'
        customer = stripe.Customer.retrieve(customer_id)
        row['cust_created'] = customer['created']
        row['description'] = customer['description']
        email = customer['email']
        if email is None:
            x = len("Customer for ")
            email = row['description'][x:]
        row['email'] = email
        data = customer['sources']['data']
        if customer['shipping'] is not None:
            print(customer['shipping'])
        if len(data) != 1:
            print("CONFUSED WITH CUSTOMER " + customer_id)
        adr = data[0]
        row['line1'] = adr['address_line1']
        row['line2'] = adr['address_line2']
        row['city'] = adr['address_city']
        row['state'] = adr['address_state']
        row['postal'] = adr['address_zip']
        row['brand'] = adr['brand']
        row['country'] = adr['country']
        row['funding'] = adr['funding']
        row['last4'] = adr['last4']
        rows.append(row)
    df2 = pd.DataFrame(rows)
    return df2


def get_subscriptions(data):
    subscribers = []
    for subscription in data:
        id = subscription['id']
        quantity = subscription['quantity']
        start = subscription['start']
        created = subscription['created']
        plan = subscription['plan']['name']
        amt = subscription['plan']['amount']
        status = subscription['status']
        customer_id = subscription['customer']
        subscriber = {"id": id, "quantity": quantity, "start": start, "created": created, "plan": plan, "amt": amt, "status": status, "customer_id": customer_id, "src": "stripe"}
        subscribers.append(subscriber)
        starting_after = id
    return subscribers

def update_stripe_subscribers(lim, e):
    subscribers = []
    q = "SELECT id FROM stripe_subscribers WHERE src='stripe' ORDER BY subscriber_id desc limit 1"
    dfx = pd.read_sql(q, e)
    starting_after = None
    if dfx.shape[0] > 0:
        starting_after = dfx.iloc[0]['id']
    if starting_after is not None:
        subscriptions = stripe.Subscription.list(limit=lim, starting_after=starting_after)
    else:
        subscriptions = stripe.Subscription.list(limit=lim)
    subs = get_subscriptions(subscriptions['data'])
    subscribers.extend(subs)
    has_more = subscriptions['has_more']
    while has_more:
        subscriptions = stripe.Subscription.list(limit=lim, starting_after=starting_after)
        subs = get_subscriptions(subscriptions['data'])
        subscribers.extend(subs)
        has_more = subscriptions['has_more']
    if len(subscribers) > 0:
        df = pd.DataFrame(subscribers)
        df['amt'] = df['amt'] / 100
        df['created'] = pd.to_datetime(df['created']*1000000000)
        df['start'] = pd.to_datetime(df['start']*1000000000)
        return df
    else:
        return None

if __name__ == '__main__':
    config = json.load(open('../config/config.json', 'r'))
    db = config['db']
    conn_template = 'mysql+pymysql://{}:{}@{}:{}/{}'
    connstr = conn_template.format(db['username'], db['password'], db['host'], db['port'], db['dbname'])
    e = sqlalchemy.create_engine(connstr)
    stripe.api_key = config['stripe_id']
    lim = 3
    df = update_stripe_subscribers(lim, e)
    if df is not None:
        print("Found", df.shape[0], "subscribers to upload.")
        df2 = update_stripe_customers(e, df)
        del df2['cust_created']
        df2.to_sql('stripe_subscribers', e, if_exists='append', index=False)
    df = update_stripe_customers(lim, e)
        