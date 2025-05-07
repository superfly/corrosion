CREATE TABLE consul_services (
    node TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    meta TEXT NOT NULL DEFAULT '{}',
    port INTEGER NOT NULL DEFAULT 0,
    address TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL DEFAULT 0,
    source TEXT,
    team_id INTEGER AS (CAST(JSON_EXTRACT(meta, '$.team_id') AS INTEGER)),
    net_id INTEGER AS (
        CAST(JSON_EXTRACT(meta, '$.net_id') AS INTEGER)
    ),
    team_name TEXT AS (JSON_EXTRACT(meta, '$.team_name')),
    deployment_id TEXT AS (
        COALESCE(
            JSON_EXTRACT(meta, '$.deployment_id'),
            SUBSTR(JSON_EXTRACT(meta, '$.alloc_id'), 1, 8)
        )
    ),
    protocol TEXT AS (JSON_EXTRACT(meta, '$.protocol')),
    PRIMARY KEY (node, id)
) WITHOUT ROWID;

CREATE TABLE consul_checks (
    node TEXT NOT NULL,
    id TEXT NOT NULL,
    service_id TEXT NOT NULL DEFAULT '',
    service_name TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    output TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL DEFAULT 0,
    source TEXT,
    PRIMARY KEY (node, id)
) WITHOUT ROWID;
