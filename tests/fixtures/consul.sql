CREATE TABLE IF NOT EXISTS consul_services (
    node TEXT NOT NULL DEFAULT "",
    id TEXT NOT NULL DEFAULT "",
    name TEXT NOT NULL DEFAULT "",
    tags JSON NOT NULL DEFAULT '[]',
    meta JSON NOT NULL DEFAULT '{}',
    port INTEGER NOT NULL DEFAULT 0,
    address TEXT NOT NULL DEFAULT "",
    updated_at BIGINT NOT NULL DEFAULT 0,
    app_id INTEGER AS (CAST(JSON_EXTRACT(meta, '$.app_id') AS INTEGER)),
    network_id INTEGER AS (
        CAST(JSON_EXTRACT(meta, '$.network_id') AS INTEGER)
    ),
    app_name TEXT AS (JSON_EXTRACT(meta, '$.app_name')),
    instance_id TEXT AS (
        COALESCE(
            JSON_EXTRACT(meta, '$.machine_id'),
            SUBSTR(JSON_EXTRACT(meta, '$.alloc_id'), 1, 8),
            CASE
                WHEN INSTR(id, '_nomad-task-') = 1 THEN SUBSTR(id, 13, 8)
                ELSE NULL
            END
        )
    ),
    organization_id INTEGER AS (
        CAST(
            JSON_EXTRACT(meta, '$.organization_id') AS INTEGER
        )
    ),
    PRIMARY KEY (node, id)
) WITHOUT ROWID;

CREATE INDEX consul_services_node_id_updated_at ON consul_services (node, id, updated_at);

CREATE INDEX consul_services_app_id ON consul_services (app_id);

CREATE INDEX consul_services_app_id_network_id ON consul_services (app_id, network_id);

CREATE INDEX consul_services_app_name_network_id ON consul_services (app_name, network_id);

CREATE INDEX consul_services_app_id_instance_id ON consul_services (app_id, instance_id);

CREATE INDEX consul_services_app_id_node ON consul_services (app_id, node);

CREATE INDEX consul_services_instance_id ON consul_services (instance_id);

CREATE TABLE IF NOT EXISTS consul_checks (
    node TEXT NOT NULL DEFAULT "",
    id TEXT NOT NULL DEFAULT "",
    service_id TEXT NOT NULL DEFAULT "",
    service_name TEXT NOT NULL DEFAULT "",
    name TEXT NOT NULL DEFAULT "",
    status TEXT,
    output TEXT NOT NULL DEFAULT "",
    updated_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (node, id)
) WITHOUT ROWID;

CREATE INDEX consul_checks_node_id_updated_at ON consul_checks (node, id, updated_at);