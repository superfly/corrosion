CREATE TABLE todos (
    id BLOB NOT NULL PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    completed_at INTEGER
);

CREATE TABLE IF NOT EXISTS testsbool (
    id INTEGER NOT NULL PRIMARY KEY,
    b boolean not null default false
);