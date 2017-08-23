CREATE TABLE account (
  id SERIAL PRIMARY KEY,
  name text,
  last_edited_by integer,
  created_on timestamp not null default now()
);

CREATE TABLE member (
  id serial PRIMARY KEY ,
  name text,
  account_id integer not null REFERENCES account(id)
);

ALTER TABLE account ADD CONSTRAINT account_last_edit_by_fkey FOREIGN KEY (last_edited_by) REFERENCES member(id);
