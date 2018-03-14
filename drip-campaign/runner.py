import sys
import pandas as pd
import sqlalchemy
import logging
import boto3
import os
from os.path import join, dirname
from datetime import datetime
from dotenv import load_dotenv

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
stdout.setFormatter(formatter)
logger.addHandler(stdout)


dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

query_get_all_users = """
    SELECT drip_user_id, email, email_count, email_last_sent, max_frequency_hours 
    FROM drip_user 
    WHERE unsubscribed=0
"""

query_get_all_send_rules = """
    SELECT send_rule_id, email_template_id, rule_priority, n_hours_after_tag_added, after_tag_id
     , int_val_lt, int_val_gt
     , date_val_lt, date_val_gt
     , double_val_lt, double_val_gt
     , has_seen_template_id, has_not_seen_template_id
    FROM drip_send_rule
    WHERE active=1
"""

query_get_all_tag_xrefs = """
    SELECT tag_id, drip_user_id, int_val, date_val, double_val, date_created
    FROM drip_user_tag_xref
"""

query_get_all_templates = """
    SELECT email_template_id, template_file_location, send_cap
    FROM drip_email_template
"""

insert_into_drip_send = """
    INSERT INTO drip_send (drip_user_id, send_rule_id, email_template_id, rendered_output_location)
    VALUES ({drip_user_id}, {send_rule_id}, {email_template_id}, '{rendered_output_location}')
"""

def get_all_users(conn):
    logger.debug("get_all_users")
    df = pd.read_sql(query_get_all_users, conn)
    users = []
    for r in range(df.shape[0]):
    	row = df.iloc[r]
    	user = row.to_dict()
    	users.append(user)
    logger.info('Found ' + str(len(users)) + ' users.')
    return users

def get_all_rules(conn):
    logger.debug("get_all_rules")
    df = pd.read_sql(query_get_all_send_rules, conn)
    rules = []
    for r in range(df.shape[0]):
        rule = df.iloc[r].to_dict()
        rules.append(rule)
    logger.info('Found ' + str(len(rules)) + ' rules.')
    return rules

def get_all_tag_xrefs(conn):
    logger.debug("get_all_tag_xrefs")
    df = pd.read_sql(query_get_all_tag_xrefs, conn)
    d = {}
    logger.info('Found ' + str(df.shape[0]) + ' tag xrefs')
    for r in range(df.shape[0]):
        row = df.iloc[r]
        row_d = row.to_dict()
        drip_user_id = row_d['drip_user_id']
        int_val = row_d['int_val']
        date_val = row_d['date_val']
        date_created = row_d['date_created']
        double_val = row_d['double_val']
        tag_id = row_d['tag_id']
        if not(drip_user_id in d):
            d[drip_user_id] = {}
        values = {"int_val": int_val, "date_val": date_val, "double_val": double_val, "date_created": date_created}
        d[drip_user_id][tag_id] = values
    logger.info('Tags cover ' + str(len(d)) + ' users.')
    return d

def get_template(s3, bucketname, s3key):
    fname = 'temp.tpl'
    s3.Bucket(bucketname).download_file(s3key, fname)
    f = open('temp.tpl', 'r')
    s = f.read()
    f.close()
    os.remove(fname)
    return s

def get_all_templates(conn, s3, bucketname):
    logger.debug("get_all_templates")
    df = pd.read_sql(query_get_all_templates, conn)
    d = {}
    for r in range(df.shape[0]):
        row = df.iloc[r]
        email_template_id = row['email_template_id']
        template_file_location = row['template_file_location']
        template = get_template(s3, bucketname, template_file_location)
        d[email_template_id] = {"metadata": row.to_dict(), "template": template}
    logger.info('Found ' + str(len(d)) + ' templates.')
    return d

def check_value_between(val, low, high):
    if low is None and high is None:
        return True
    if val is None and (low is not None or high is not None):
        return False
    if low is not None:
        if val < low:
            return False
    if high is not None:
        if val > high:
            return False
    return True

def user_template_sent_count(conn, user, drip_user_id, template_id):
    tpl = "SELECT count(*) as c from drip_send WHERE drip_user_id={drip_user_id} and email_template_id={email_template_id}"
    query = tpl.format(drip_user_id=drip_user_id, email_template_id=template_id)
    df = pd.read_sql(query, conn)
    c = df.iloc[0]['c']
    logger.info('Counted ' + str(c) + ' templates sent.')
    return c

def check_rule(now, conn, rule, template, user, user_tags):
    logger.debug("check_rule")
    template_id = rule['email_template_id']
    drip_user_id = user['drip_user_id']
    has_seen_count = user_template_sent_count(conn, user, drip_user_id, template_id)
    print(template)
    send_cap = template['metadata']['send_cap']
    if has_seen_count >= send_cap:
        return None
    after_tag_id = rule['after_tag_id']
    tag = None
    if after_tag_id is not None:
        tag = user_tags[after_tag_id]
        if tag is None:
            return None
    n_hours_after_tag_added = rule['n_hours_after_tag_added']
    tagged_at = None
    if tag:
        tagged_at = tag['date_created']
        delta = now - tagged_at
        hours_since_tag = d.seconds/60/60
        if hours_since_tag < n_hours_after_tag_added:
            return None
        tag_id = tag['tag_id']
        user_tag = user_tags[tag_id]
        int_val = user_tag['int_val']
        date_val = user_tag['date_val']
        double_val = user_tag['double_val']
        send = check_value_between(int_val, rule['int_val_lt'], rule['int_val_gt'])
        if not(send):
            return None
        send = check_value_between(date_val, rule['date_val_lt'], rule['date_val_gt'])
        if not(send):
            return None
        send = check_value_between(double_val, rule['double_val_lt'], rule['double_val_gt'])
        if not(send):
            return None
    template_id = rule['has_seen_template_id']
    if template_id is not None:
        has_seen_count = user_template_sent_count(conn, user, drip_user_id, template_id)
        if has_seen_count == 0:
            return None
    template_id = rule['has_not_seen_template_id']
    if template_id is not None:
        has_seen_count = user_template_sent_count(conn, user, drip_user_id, template_id)
        if has_seen_count > 0:
            return None
    priority = rule['rule_priority']
    email_template_id = rule['email_template_id']
    return {"priority": priority, "email_template_id": email_template_id}

def get_content_from_s3(s3, bucketname, s3key):
    logger.debug("get_content_from_s3")
    try:
        filename = 'temp.htm'
        s3.Bucket(bucketname).download_file(s3key, filename)
        f = open(filename, 'r')
        s = f.read()
        f.close()
        os.remove(filename)
        return s
    except botocore.exceptions.ClientError as e:
        logger.error('Count not get ' + s3key)
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        return None

def save_personalize_content(now, s3, bucketname, content, template_file_location):
    # TODO: personalize the content
    template_name = template_file_location.replace('/', '_')
    yyyy_mm = str(now)[0:7]
    dd_hh_ii_ss = str(now)[8:19].replace(':', '_').replace(' ', '_')
    s3key = 'emails/{email}/{yyyy_mm}/{dd_hh_ii_ss}-{template_name}'
    logger.debug("save_personalize_content to " + s3key)
    obj = s3.Object(bucketname, s3key)
    obj.put(Body=content)
    return s3key

def personalize_content(content):
    return content

def save_rendered_template(now, s3, bucketname, template):
    logger.debug("save_rendered_template")
    print(template, '!')
    template_file_location = template['metadata']['template_file_location']
    content = get_content_from_s3(s3, bucketname, template_file_location)
    if content is None:
        return None
    content2 = personalize_content(content)
    rendered_output_location = save_personalize_content(now, s3, bucketname, content2, template_file_location)
    return rendered_output_location

def queue_to_send(now, s3, conn, bucketname, drip_user_id, send_rule_id, template):
    logger.debug("queue_to_send")
    print(drip_user_id, send_rule_id, template)
    rendered_output_location = save_rendered_template(now, s3, bucketname, template)
    if rendered_output_location is not None:
        t = insert_into_drip_send
        email_template_id = template['metadata']['email_template_id']
        query = t.format(drip_user_id=drip_user_id, send_rule_id=send_rule_id, email_template_id=email_template_id, rendered_output_location=rendered_output_location)
        conn.execute(query)
    else:
        print("ERROR: could not render.")
        # TODO: call to TSE

def do_mail_run(now, conn, s3, bucketname):
    logger.debug("do_mail_run")
    users     = get_all_users(conn)
    rules     = get_all_rules(conn)
    tags      = get_all_tag_xrefs(conn)
    templates = get_all_templates(conn, s3, bucketname)
    for user in users:
        drip_user_id = user['drip_user_id']
        if drip_user_id in tags:
            user_tags = tags[drip_user_id]
            max_priority = 0
            max_send_rule_id = -1
            email_to_send = None
            for rule in rules:
                email_template_id = rule['email_template_id']
                template = templates[email_template_id]
                result = check_rule(now, conn, rule, template, user, user_tags)
                if result is not None:
                    priority = result['priority']
                    if priority > max_priority:
                        max_priority = priority
                        max_send_rule_id = rule['send_rule_id']
                        email_to_send = result
            if email_to_send is not None:
                email_template_id = email_to_send['email_template_id']
                template = templates[email_template_id]
                queue_to_send(now, s3, conn, bucketname, drip_user_id, max_send_rule_id, template)


if __name__ == '__main__':
    logger.info("Init")
    bucketname = os.environ.get('BUCKETNAME')
    user = os.environ.get('DB_USER')
    passwd = os.environ.get('DB_PASSWORD')
    host = os.environ.get('DB_HOST')
    port = os.environ.get('DB_PORT')
    name = os.environ.get('DB_NAME')
    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')
    conn_template = 'mysql://{}:{}@{}:{}/{}'
    connstr = conn_template.format(user, passwd, host, port, name)
    conn = sqlalchemy.create_engine(connstr)
    bucketname = os.environ.get('BUCKETNAME')
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    now = datetime.now()
    do_mail_run(now, conn, s3, bucketname)
    logger.info("Complete")

