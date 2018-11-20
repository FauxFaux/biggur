create table item (
  id varchar primary key,
  body varchar not null,
  observed datetime not null
);

create table item_children (
  item varchar not null,
  child varchar not null
);

create unique index item_child on item_children (item, child);

-- append-only list of when we've observed e.g. most viral, for provisioning ids
create table hot_observation (
  id integer primary key autoincrement,
  name varchar not null,      -- viral, rising, etc.
  observed datetime not null
);

create table hot_content (
  hot_id integer not null,
  item_id varchar not null,
  pos integer not null        -- index within the hot
);
