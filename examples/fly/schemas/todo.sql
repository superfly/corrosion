CREATE TABLE todos (
    id BLOB NOT NULL PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    completed_at TEXT DEFAULT 100
);
