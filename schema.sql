-- tables for IdempotentExample

create table idem_data(
  msg character varying(255),
  primary key (msg)
);

-- postgres isnt the best for idempotent storage, this is for example purposes only
create or replace rule idem_data_ignore_duplicate_inserts as
  on insert to idem_data
  where (exists (select 1 from idem_data where idem_data.msg = new.msg))
  do instead nothing
;


-- tables for TransactionalExample
create table txn_data(
  topic character varying(255),
  metric bigint
);

create table txn_offsets(
  topic character varying(255),
  part integer,
  off bigint,
  unique (topic, part)
);

insert into txn_data(topic, metric) values
  ('test', 0)
;

insert into txn_offsets(topic, part, off) values
-- or whatever your initial offsets per partition are, if non-0
  ('test', 0, 0),
  ('test', 1, 0)
;
