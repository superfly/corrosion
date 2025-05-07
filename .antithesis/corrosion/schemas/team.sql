CREATE TABLE teams (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL default '',
    net_id BIGINT NOT NULL default 0,
    role text,
    settings text not null default '{}',
    state text not null default 'creating',
    balancer_ip text,
    created_at BIGINT NOT NULL default 0,
    updated_at BIGINT NOT NULL default 0
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER NOT NULL PRIMARY KEY,
    name TEXT NOT NULL DEFAULT "",
    email TEXT NOT NULL DEFAULT "",
    team_id INTEGER NOT NULL default 0,
    encoded_id TEXT,
    created_at BIGINT NOT NULL default 0,
    updated_at BIGINT NOT NULL default 0,
    status TEXT
);
