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
	SELECT tag_id, drip_user_id, int_val, date_val, double_val
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
	df = pd.read_sql(query_get_all_users, conn)
	return df

def get_all_rules(conn):
	df = pd.read_sql(query_get_all_send_rules, conn)
	return df

def get_all_tag_xrefs(conn):
	df = pd.read_sql(query_get_all_tag_xrefs, conn)
	d = {}
	for r in range(df.shape[0]):
		row = df.iloc[r]
		row_d = row.to_dict()
		if not(drip_user_id in d):
			d[drip_user_id] = {}
		values = {"int_val": int_val, "date_val": date_val, "double_val": double_val}
		d[drip_user_id][tag_id] = values
	return d

def get_all_templates(conn, s3):
	df = pd.read_sql(query_get_all_templates, conn)
	d = {}
	for r in range(df.shape[0]):
		row = df.iloc[r]
		email_template_id = row['email_template_id']
		template_file_location = row['template_file_location']
		template = get_template(s3, template_file_location)
		d[email_template_id] = {"metadata": row.to_dict(), "template": template}
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

def user_has_seen_template_id(conn, user, template_id):
	tpl = "SELECT count(*) as c from drip_send WHERE drip_user_id={drip_user_id} and email_template_id={email_template_id}"
	query = tpl.format(drip_user_id=drip_user_id, email_template_id=email_template_id)
	df = pd.read_sql(query, conn)
	if df.iloc[0]['c'] == 0:
		return False
	else:
		return True

def check_rule(conn, rule, user, user_tags)
	after_tag_id = rule['after_tag_id']
	tag = user_tags[after_tag_id]
	if tag is None:
		return None
	n_hours_after_tag_added = rule['n_hours_after_tag_added']
	hours_since_tag = ?
	if hours_since_tag < n_hours_after_tag_added:
		return None
	int_val = user_tags['int_val']
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
	has_seen = user_has_seen_template_id(conn, user, template_id)
	if not(has_seen):
		return None
	template_id = rule['has_not_seen_template_id']
	has_seen = user_has_seen_template_id(conn, user, template_id)
	if has_seen:
		return None
	priority = rule['rule_priority']
	email_template_id = rule['email_template_id']
	return {"priority": priority, "email_template_id": email_template_id}

def save_rendered_template(?):
	return ?

def email_to_send(drip_user_id, send_rule_id, email_template_id):
	rendered_output_location = save_rendered_template(?)
	t = insert_into_drip_send
	t.format(drip_user_id=drip_user_id, send_rule_id=send_rule_id, email_template_id=email_template_id, rendered_output_location=rendered_output_location)

def do_mail_run(conn, s3):
	users     = get_all_users(conn)
	rules     = get_all_rules(conn)
	tags      = get_all_tag_xrefs(conn)
	templates = get_all_templates(conn, s3)
	for user in users:
		drip_user_id = user['drip_user_id']
		user_tags = tags[drip_user_id]
		max_priority = 0
		max_send_rule_id = -1
		email_to_send = None
		for rule in rules:
			result = check_rule(conn, rule, user, user_tags)
			if result is not None:
				priority = result['priority']
				if priority > max_priority:
					max_priority = priority
					max_send_rule_id = rule['send_rule_id']
					email_to_send = result
		if email_to_send is not None:
			email_template_id = email_to_send['email_template_id']
			queue_to_send(drip_user_id, max_send_rule_id, email_template_id)


if __name__ == '__main__':
	conn = ?
	s3 = ?
	do_mail_run(conn, s3)

