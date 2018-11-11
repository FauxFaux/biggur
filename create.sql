create table raw (
  occurred timestamp not null,
  url varchar not null,
  returned varchar not null
);

create unique index raw_fetch on raw (url, occurred);
