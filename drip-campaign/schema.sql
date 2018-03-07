use dataskeptic_dev

CREATE TABLE drip_user (
  drip_user_id bigint not null AUTO_INCREMENT
, email varchar(256) not NULL
, unsubscribed int not null default 0
, email_count int not null default 0
, email_last_sent datetime null
, max_frequency_hours int not null default 24
, date_created timestamp default '0000-00-00 00:00:00'
, last_updated timestamp default now() on update now()
, PRIMARY KEY (drip_user_id)
);

CREATE TABLE drip_tag (
  tag_id BIGINT NOT NULL AUTO_INCREMENT
, tag_name varchar(256) not NULL
, descrip varchar(1024) not null default ''
, date_created timestamp default now()
, PRIMARY KEY (tag_id)
);

CREATE TABLE drip_user_tag_xref (
  xref_id BIGINT NOT NULL
, tag_id BIGINT not NULL
, drip_user_id BIGINT NOT NULL
, int_val bigint NULL
, date_val datetime NULL
, double_val double not null
, date_created timestamp default now()
);

create index idx_user_tag_xref_user on drip_user_tag_xref (drip_user_id);

create index idx_user_tag_xref_tag on drip_user_tag_xref (tag_id);

CREATE TABLE drip_send (
  send_id bigint not null AUTO_INCREMENT
, drip_user_id bigint not NULL
, email_template_id bigint not null
, rendered_output_location varchar(1024) not null
, date_created timestamp default now()
, sent_status int not null default 0
, send_grid_result varchar(1024) null
, PRIMARY KEY (send_id)
);

create index idx_drip_send_template_user on drip_send (drip_user_id, email_template_id);

CREATE TABLE drip_email_template (
  email_template_id BIGINT not NULL AUTO_INCREMENT
, template_file_location varchar(1024) not null
, send_cap int not null default 1
, date_created timestamp default now()
, PRIMARY KEY (email_template_id)
);

CREATE TABLE drip_send_rule (
  send_rule_id BIGINT NOT NULL AUTO_INCREMENT
, email_template_id bigint not null
, rule_priority double NOT NULL default 1.0
, n_hours_after_tag_added double null
, after_tag_id bigint null
, int_val_gt bigint NULL
, int_val_lt bigint NULL
, date_val_gt datetime NULL
, date_val_lt datetime NULL
, double_val_gt double not null
, double_val_lt double not null
, has_seen_template_id bigint NULL
, has_not_seen_template_id bigint null
, date_created timestamp default '0000-00-00 00:00:00'
, last_updated timestamp default now() on update now()
, PRIMARY KEY (send_rule_id)
);