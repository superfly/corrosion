CREATE TABLE deployments (
    id INTEGER NOT NULL PRIMARY KEY,
    node TEXT NOT NULL default '',
    name TEXT NOT NULL default '',
    version TEXT NOT NULL default '',
    status text not null default '',
    settings text not null default '{}',

    egress_ip text not null default '',
    ingress_ip text not null default '',

    team_id INTEGER NOT NULL default 0,
    net_id INTEGER NOT NULL default 0,
    created_at INTEGER NOT NULL default 0,
    updated_at INTEGER NOT NULL default 0
) WITHOUT ROWID;
