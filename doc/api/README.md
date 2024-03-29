# API

Each running Corrosion agent hosts a RESTful HTTP API for interacting with the cluster's synchronized database. Endpoints accept SQL statements in a JSON body, for versatility.

Each request is run in a transaction (as if the  `transaction` query param had been passed).

Endpoints:

- [POST /v1/transactions](transactions.md) for writes
- [POST /v1/queries](queries.md) for reads
- [POST /v1/subscriptions](subscriptions.md) to receive streaming updates for a desired query