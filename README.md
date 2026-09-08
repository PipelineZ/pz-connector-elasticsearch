# Pz.Connector.Elasticsearch

Elasticsearch source and sink for [PipelineZ](https://pipelinez.dev) (`pz`), served out of process.
An index reads as a **table**: its mapping is the schema, documents are paged through a point in
time, and the engine's incremental watermarks become a `range` filter. A sink output **appends**,
**merges** by `_id`, or **replaces** an alias atomically.

## Installation

```yaml
# project.yml
connectors:
  - package: Pz.Connector.Elasticsearch
    version: 0.1.0
```

`pz restore` installs the Native AOT binary for your platform (linux-x64, linux-arm64, osx-arm64,
win-x64) and `pz run` spawns it. Needs pz 0.6.0 or newer. Built on the official
`Elastic.Clients.Elasticsearch` 9.x client; tested against Elasticsearch 9.5.

| RID | status |
|---|---|
| `linux-x64` | the platform every test and the packaging proof run on |
| `linux-arm64`, `osx-arm64`, `win-x64` | shipped, never exercised by this connector's own CI |

## Connection

```yaml
# connections.yml
search:
  connector: elasticsearch
  url: https://es.example:9200        # required; one node
  api_key: ${ES_API_KEY}              # the base64 "encoded" value Elasticsearch hands out
  # -- or --
  username: ${ES_USER}
  password: ${ES_PASSWORD}
  ca_cert: certs/http_ca.crt          # optional; relative paths resolve against the project directory
  ca_fingerprint: a52dd935...         # optional; the HTTP CA SHA-256 fingerprint printed at first start
  insecure: true                      # optional; skip server certificate validation (dev only)
  timeout: 60                         # optional; seconds per request
```

`api_key` and `username`/`password` are exclusive; so are `ca_cert`, `ca_fingerprint` and
`insecure`, and each of those needs an `https` url. No credentials at all is fine for a cluster
with security disabled. The api key and the password are redacted from every error.

## Reading an index

```yaml
  entities:
    orders:
      read:
        index: orders-*             # optional; defaults to the entity name; an alias, index, or pattern
        query:                      # optional; Elasticsearch Query DSL, as YAML or a JSON string
          term: { status: shipped }
        page_size: 1000             # optional; 1..10000 documents per page
        json_fields: [tags, items]  # optional; field paths landed as JSON text instead of typed
        pit_keep_alive: 5m          # optional; how long each page may take before the snapshot expires
```

The columns are the mapping's fields, depth-first in the order Elasticsearch lists them, then one
trailing `_id`:

| mapping type | column type | value |
|---|---|---|
| `integer`, `short`, `byte` | integer | a number, or a numeric string |
| `long`, `unsigned_long` | bigint | same |
| `float`, `half_float`, `double`, `scaled_float` | double | same |
| `boolean` | boolean | `true`/`false`, as a JSON boolean or string |
| `date`, `date_nanos` with the default or a standard format | timestamp (UTC) | ISO-8601 (no zone = UTC), or an epoch number |
| `date` with any other `format` | varchar | the source text, unparsed |
| `keyword`, `text`, `wildcard`, `ip`, `version`, `binary`, ... | varchar | the string |
| `object` with properties | flattened into `parent.child` columns | |
| `nested`, `flattened`, `geo_*`, vectors, ranges, anything else | varchar | the field's JSON |
| anything under `json_fields` | varchar | the field's JSON, and an object listed there is not flattened |

Standard date formats: `strict_date_optional_time`, `strict_date_optional_time_nanos`,
`date_optional_time`, `epoch_millis`, `epoch_second`, in any `||` combination.

A field that holds an **array** where the mapping declares a scalar fails the read naming the field
and the document: list it under `json_fields` and decode it in SQL
(`json_extract_string(tags, '$[0]')`). An `index:` pattern spanning indices whose mappings disagree
on a field's type is refused the same way.

**Incremental reads.** Declare the cursor in SQL as for any pz source:

```sql
select * from {{ source('search', 'orders') }}
where updated_at > {{ watermark('search', 'orders') }}
```

The cursor must be a numeric or standard-format `date` field; the bound (and a bounded window's
upper bound) becomes a `range` clause in the same `bool.filter` as your `query:`. Column pruning is
honoured through `_source.includes`; there is no SQL predicate pushdown -- `query:` is the explicit
lever for that.

Each read opens a point in time, so it sees one consistent version of the index however long it
pages; the connector reads through the client's transport rather than the typed search API so the
binary stays Native AOT.

## Writing an index

```yaml
  entities:
    orders_out:
      write:
        index: orders               # optional; defaults to the entity name
        strategy: append            # append | merge | replace
        keys: [tenant, order_id]    # merge only
        bulk_size: 1000             # optional; documents per _bulk request
        bulk_bytes: 5242880         # optional; bytes per _bulk request
```

Every row becomes one JSON document of every column: integers and doubles as numbers, decimals as
strings (every digit), booleans, dates as `yyyy-MM-dd`, timestamps as `yyyy-MM-ddTHH:mm:ss.ffffffZ`,
nulls as `null`. A dotted column name becomes a dotted key, which Elasticsearch treats as an object
path. Columns outside pz's type matrix are refused before a request is sent.

- **`append`**: Elasticsearch assigns each document's `_id`. At-least-once across runs, as for every
  `append` output; the index is created on first write with dynamic mapping unless you created it.
- **`merge`**: `_id` is the key column's value (composite keys join with `|`), and a full-document
  index is an upsert -- the row replaces the document. A null key value fails the write.
- **`replace`**: the output name must be an **alias** (or not exist yet). The write goes to a fresh
  index named `<alias>-pz-<timestamp>-<suffix>`, created with the current write index's mapping;
  commit refreshes it and issues one `_aliases` request that points the alias at it and deletes every
  index previously behind it -- readers see the old set or the new one, never a mix. A concrete index
  of the output name is refused rather than deleted; reindex it behind an alias first.

Commit flushes the last bulk and refreshes the target, so a downstream read in the same run sees
the write. A bulk answer that rejects any item fails the write with the item's `type: reason`;
`429` rejections are transient and retried by the engine. Abort deletes a replace's staging index;
an aborted append or merge cannot unsend the bulks it already delivered.

## Errors

Every failure is `elasticsearch: <what was being done>: <type>: <reason> (HTTP n)`. No answer at
all, `408`, `429`, `502`, `503`, `504`, and an expired point in time are transient and retried by the
engine; credentials, authorization, a missing index, and malformed requests are not. `pz connector
check` runs `GET /` and reports the cluster name and version.

## Development

```bash
dotnet build Pz.Connector.Elasticsearch.slnx -c Release
dotnet test Pz.Connector.Elasticsearch.slnx -c Release --no-build      # node facts need docker; they SKIP without it
dotnet restore src/Pz.Connector.Elasticsearch -r linux-x64             # once, on a cold cache
dotnet publish src/Pz.Connector.Elasticsearch -c Release -r linux-x64 --no-restore
dotnet pack src/Pz.Connector.Elasticsearch -c Release -o packages      # nupkg with pz.connector.json
```

The docker facts start `elasticsearch:9.5.3` through Testcontainers (one node with security off for
the acceptance suites, one with the image's default TLS and password for the security facts).
`tests/e2e/` is the pz project CI runs against a packed nupkg. Releases are tag-triggered (`v*`) and
publish to nuget.org through trusted publishing.
