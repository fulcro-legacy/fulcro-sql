CREATE TABLE settings (
  id                 SERIAL PRIMARY KEY,
  auto_open          BOOLEAN NOT NULL DEFAULT FALSE,
  keyboard_shortcuts BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE account (
  id             SERIAL PRIMARY KEY,
  name           TEXT,
  last_edited_by INTEGER,
  settings_id    INTEGER REFERENCES settings (id),
  created_on     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE member (
  id         SERIAL PRIMARY KEY,
  name       TEXT,
  account_id INTEGER NOT NULL REFERENCES account (id)
);
ALTER TABLE account
  ADD CONSTRAINT account_last_edit_by_fkey FOREIGN KEY (last_edited_by) REFERENCES member (id);

CREATE TABLE invoice (
  id           SERIAL PRIMARY KEY,
  invoice_date TIMESTAMP NOT NULL DEFAULT now(),
  account_id   INTEGER   NOT NULL REFERENCES account (id)
);

CREATE TABLE item (
  id   SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE invoice_items (
  id         SERIAL PRIMARY KEY,
  quantity   SMALLINT NOT NULL,
  invoice_id INTEGER  NOT NULL REFERENCES invoice (id),
  item_id    INTEGER  NOT NULL REFERENCES item (id)
);
