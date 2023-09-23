CREATE TABLE todos (
    id BLOB PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    completed_at INTEGER
);