CREATE SEQUENCE settings_id_seq;
CREATE TABLE settings (
  id                 INTEGER DEFAULT nextval('settings_id_seq') PRIMARY KEY ,
  auto_open          BOOLEAN NOT NULL    DEFAULT FALSE,
  keyboard_shortcuts BOOLEAN NOT NULL    DEFAULT TRUE
);

CREATE SEQUENCE account_id_seq;
CREATE TABLE account (
  id             INTEGER DEFAULT nextval('account_id_seq'),
  name           VARCHAR(200),
  last_edited_by INTEGER,
  settings_id    INTEGER REFERENCES settings (id),
  created_on     TIMESTAMP NOT NULL  DEFAULT now(),
  spouse_id      INTEGER REFERENCES account (id)
);

CREATE SEQUENCE member_id_seq;
CREATE TABLE member (
  id         INTEGER DEFAULT nextval('member_id_seq'),
  name       VARCHAR(200),
  account_id INTEGER NOT NULL REFERENCES account (id)
);
ALTER TABLE account
  ADD CONSTRAINT account_last_edit_by_fkey FOREIGN KEY (last_edited_by) REFERENCES member (id);

CREATE SEQUENCE invoice_id_seq;
CREATE TABLE invoice (
  id           INTEGER DEFAULT nextval('invoice_id_seq'),
  invoice_date TIMESTAMP NOT NULL           DEFAULT now(),
  account_id   INTEGER   NOT NULL REFERENCES account (id)
);

CREATE SEQUENCE item_id_seq;
CREATE TABLE item (
  id   INTEGER DEFAULT nextval('item_id_seq'),
  name VARCHAR(200) NOT NULL
);

CREATE SEQUENCE invoice_items_id_seq;
CREATE TABLE invoice_items (
  id         INTEGER DEFAULT nextval('invoice_items_id_seq'),
  quantity   SMALLINT NOT NULL,
  invoice_id INTEGER  NOT NULL REFERENCES invoice (id),
  item_id    INTEGER  NOT NULL REFERENCES item (id)
);

CREATE SEQUENCE todo_list_id_seq;
CREATE TABLE todo_list (
  id   INTEGER DEFAULT nextval('todo_list_id_seq'),
  name VARCHAR(200)
);

CREATE SEQUENCE todo_list_item_id_seq;
CREATE TABLE todo_list_item (
  id             INTEGER DEFAULT nextval('todo_list_item_id_seq'),
  label          VARCHAR(200),
  todo_list_id   INTEGER REFERENCES todo_list (id),
  parent_item_id INTEGER REFERENCES todo_list_item (id)
);
