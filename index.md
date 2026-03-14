# Design of an Alert Retrieval Service in the RSP

```{abstract}
This document describes the design and implementation of Herald, a new alert retrieval service being added to the Rubin Science Platform (RSP).
Herald will provide authenticated HTTP access to alert packets stored in the USDF S3 alert archive and return them to clients either as deserialised JSON or as self-describing Avro files.
The service will be built using the common FastAPI framework and will be deployed via Phalanx on all the IDF and USDF environments.
The service will retrieve the packets by ID, strip the header to extract the schema ID, fetch the matching Avro schema and deserialise the payload using fastavro.
```

## 1. Introduction

Rubin will generate alert packets for every transient detection nightly. These packets are distributed to community brokers via a Kafka, but they are also stored at USDF in an S3 bucket.

The existing alert archive infrastructure consists of two components: 
- The `alert_database_ingester` (https://github.com/lsst-dm/alert_database_ingester) which consumes alerts from Kafka and writes them to S3-compatible object storage.
- The `alert_database_server` (https://github.com/lsst-dm/alert_database_server) a FastAPI service that serves the raw binary alert bytes.

The existing server returns raw gzip-compressed avro bytes.

Herald replaces and extends this with:

- Gafaelfawr authentication, as is used for the rest of the RSP applications
- Content-negotiated responses: JSON (deserialised) or Avro OCF (self-describing binary, schema embedded)
- A schema endpoint that returns the Avro schema used to encode a given alert
- Deployment as a Phalanx application following established SQRE patterns

### Goals

The goals for this service include:

- Provide RSP users with authenticated, programmatic access to archived alert packets
- Support both JSON and Avro OCF
- Reuse existing archive storage infrastructure without modifying it
- Follow SQuaRE service conventions

### Out of scope

This service will not provide search and filtering capabilities across the alert archive.
The service will accept a single alert ID and return that alert, but the initial MVP will not include batch alert requests.
The relationship between alert IDs and sky position or other attributes is the responsibility of the alert broker ecosystem and the PPDB.

## 2. Alert Archive Background

### Alert packet format

Rubin alert packets are serialised using Apache Avro (https://avro.apache.org/) with the Confluent Wire Format (https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format).
This is a binary format consisting of a 5-byte header followed by a schemaless Avro-encoded record:

```
Byte 0:     0x00          - magic byte, identifies Confluent Wire Format
Bytes 1–4:  <uint32 BE>   - schema ID
Bytes 5+:   <avro binary> - Avro-encoded record, (no schema embedded)
```

The schema is stored separately and will be identified by the 4-byte integer ID in the header.

Alert packets are stored gzip-compressed (`*.avro.gz`) in S3.
When no compressed version exists an uncompressed file (`*.avro`) is tried as a fallback.
This matches the behaviour of the existing `alert_database_server`.


### Storage layout

The ingester writes alert packets and schemas to two separate S3 buckets, following the layout defined in the ingester codebase:

**Alert packets:**
```
{alerts_bucket}/v2/alerts/{alert_id[:6]}/{alert_id}.avro.gz
```

The convention is that the first 6 characters of the alert ID string form a prefix directory.
For example, alert ID `1234567890` is stored at `v2/alerts/123456/1234567890.avro.gz`.
This sharding is presumably there to limit the number of objects per prefix directory.

The `v2/alerts` prefix is the default but is configurable via `s3_alerts_prefix`.

**Schemas:**
```
{schemas_bucket}/v2/schemas/{schema_id}.json
```

Schema files are plain UTF-8 JSON, named by the integer schema ID in decimal (e.g. `v2/schemas/1.json`).
The `v2/schemas` prefix is the default but is configurable via `s3_schemas_prefix`.
Schemas are immutable; a given schema ID always refers to the same schema.
The Rubin alert schema changes infrequently, roughly monthly at most.

### Alert ID mapping

Alert IDs currently map directly to `diaSourceId` and this mapping is expected to remain stable.

## 3. Requirements

The requirements for Herald, following the team responsible for the alert archive at USDF are as follows:

- Accept a single integer alert ID as the request parameter
- Retrieve the corresponding alert packet from USDF S3-compatible object storage
- Return the alert either as deserialised JSON or as an Avro OCF file, based on the `Accept` header
- When returning Avro, embed the schema in the response so the result is self-describing
- Expose the Avro schema for a given alert via a dedicated endpoint
- Authenticate requests using Gafaelfawr

Alert packets average 83 KB without postage stamps and 112 KB with stamps, with a worst case of approximately 500 KB.
The request pattern is not known at the current time, but there is no expectation for batch-style access pattern due to automated requests acting on given transients.
If this does occur we can handle by introducing auto-scaling and rate-limits if and where appropriate.

## 4. Architecture

Herald is a standard Safir FastAPI service following the established SQuaRE patterns.
Following is a high-level design diagram showing the various system components and how they interact.

### Architecture Diagram

```{figure} herald_sys_diagram.png
:figclass: technote-wide-content
:scale: 50%

Proposed Architecture Diagram
```

### Key classes

**`ProcessContext`** Store the process-wide singletons, in this case this will probably be the aiobotocore S3 client and the `AlertStore`.

**`AlertStore`** wraps the aiobotocore S3 client.
It fetches and gunzips alert bytes from the alerts bucket and fetches schema JSON from the schemas bucket.
Schemas are cached in a plain Python dict keyed by schema ID. Note that the number of schemas is small and the schema is expected to change infrequently (maybe once per month but less so after it is more stable)

**`Factory`** Constructs `AlertService` instances by combining the shared `AlertStore` with the request-scoped bound logger.

**`AlertService`** orchestrates the retrieval pipeline: Specifically, fetch the alert bytes, decompress, parse the header, fetch the schema, and deserialise with fastavro.
This will expose at the very least three methods: `get_alert()` returning a dict, `get_alert_avro()` returning Avro OCF bytes, and `get_alert_schema()` returning the schema dict.


## 5. API

The service will expose three endpoints under the `/api/herald` path prefix.

### `GET /api/herald/`

Returns application metadata, similar to all other RSP applications (Unauthenticated).

### `GET /api/herald/alerts/{alert_id}`

Returns the alert packet for the given integer alert ID.

The response format is controlled by the `Accept` header:

| `Accept` header | Response | Content-Type |
|---|---|---|
| `application/json` (default) | Deserialised alert record as JSON. Binary fields (e.g. cutout stamps) are base64-encoded. | `application/json` |
| `application/avro` | Avro OCF container file with the schema included. | `application/avro` |

Authentication required (`read:image` scope).

**Responses:**
- `200 OK` - alert found and returned
- `401 Unauthorized` - missing or invalid Gafaelfawr token
- `404 Not Found` - no alert exists for the given ID
- `422 Unprocessable Content` - alert bytes found but the Confluent header is malformed

### `GET /api/herald/alerts/{alert_id}/schema`

Returns the Avro schema used to encode the given alert, as a JSON object.
The schema is identified by the schema ID embedded in the alert's Confluent Wire Format header.

Authentication required (`read:image` scope).

**Responses:**
- `200 OK` - schema returned as JSON
- `404 Not Found` - alert not found, or schema ID not found in schemas bucket

## 6. Alert Deserialisation

The full deserialisation pipeline for a JSON response will be:

1. Fetch from S3 and gunzip
2. Parse Confluent Wire Format header
3. Fetch schema (in-memory cached)
4. Deserialise

For an Avro OCF response, after step 4 the record is re-serialised into an OCF container file, and in the process we embed the full schema in the file header.
This makes the response self-describing and allows clients to use any Avro library to read it without a separate schema request.

The OCF approach was chosen over returning raw Confluent Wire Format bytes because it embeds the schema, matching the requirement that Avro responses be self-describing.

### Binary fields

Alert packets include postage stamp cutout images as raw bytes fields.
When returning JSON byte fields are base64 encoded so the response is valid JSON.
When returning Avro OCF byte fields are preserved as they were.

## 7. Scope

For our initial implementation the alert endpoint will require the `read:image` scope, which is also used by Butler, vo-cutouts, datalinker etc. 
Users who have `read:image` are probably the intended audience for this service, so reusing the scope avoids provisioning overhead for now.

We may also want to consider a new scope `read:alert` if we want a more accurate and fine-grained scope for this service.
For example we may want to consider allowing a community broker or automated pipeline service account to retrieve alerts without granting full RSP data rights. If this is ever the case it may make sense to introduce a new scope, but this should be possible without any changes to the Herald application itself.

## 8. Storage Configuration

Herald is configured with two separate S3 bucket names (matching the ingester's two-bucket layout) and an optional endpoint URL override for non-AWS S3-compatible stores (like the USDF S3).

Relevant configuration fields (all prefixed `HERALD_` as ENV variables):

| Field | Description |
|---|---|
| `s3_alerts_bucket` | Bucket containing alert packet objects |
| `s3_schemas_bucket` | Bucket containing Avro schemas as JSON objects |
| `s3_alerts_prefix` | S3 key prefix for alert packets (default: `v2/alerts`) |
| `s3_schemas_prefix` | S3 key prefix for Avro schemas (default: `v2/schemas`) |
| `s3_endpoint_url` | Override endpoint for S3-compatible stores |
| `aws_access_key_id` | S3 access key ID (optional; use IAM roles where available) |
| `aws_secret_access_key` | S3 secret access key (optional) |
| `s3_region` | S3 region (optional; required for AWS S3 and GCS S3-compatible API) |

## 9. Phalanx Deployment

Herald will be deployed as a Phalanx application at `applications/herald/` using the known phalanx template & structure.


## 10. Implementation Notes

### Schema caching

The in-process schema cache (`dict[int, bytes]`) should be sufficient since schemas are immutable by convention, so there should be no invalidation concern.
Given that the Rubin alert schema changes roughly once a month, the cache will remain small for the lifetime of the process and thus a more complex shared cache (Redis, etc.) does not seem warranted.

### `.avro` fallback

The storage layer tries the gzip-compressed key first (`*.avro.gz`).  If S3 returns `NoSuchKey`, we will retry with the uncompressed key (`*.avro`) to match the behaviour of the existing `alert_database_server`.  We may choose to later remove this complexity if we think that this will never occur in the future.

### Avro OCF vs raw Confluent Wire Format

When the user requests the Avro format, returning the Confluent Wire Format bytes stored in S3 are not very useful to a client without the schema. 
Thus we will fetch the 4-byte schema ID and resolve it using the schemas bucket to decode the payload. The choice of Avro OCF as the binary response format is due to the fact that it embeds the full schema, and thus makes the response immediately usable by any Avro library without any additional requests.

### Error handling

| Condition | HTTP status |
|---|---|
| Alert ID not found in S3 | 404 |
| Schema ID not found in S3 | 404 |
| Confluent magic byte is wrong | 422 |
| Alert payload shorter than 5 bytes | 404 (treated as not found) |

## 11. System Flow Chart

Following a flow-chart to visualize how the request will flow through Herald and the S3 storage layer.

```{figure} herald_flow.png
:figclass: technote-wide-content
:scale: 50%

 System Flow Chart
```

## References

- alert_database_ingester: https://github.com/lsst-dm/alert_database_ingester
- alert_database_server: https://github.com/lsst-dm/alert_database_server
- Herald (this service): https://github.com/lsst-sqre/herald
- Phalanx deployment: https://github.com/lsst-sqre/phalanx
- Safir: https://safir.lsst.io/
- Gafaelfawr: https://gafaelfawr.lsst.io/
- fastavro: https://fastavro.readthedocs.io/
- Apache Avro specification: https://avro.apache.org/docs/current/spec.html
- Confluent Wire Format: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
- DMTN-183: Alert Production Pipeline Design
