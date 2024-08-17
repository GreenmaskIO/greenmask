## show-dump command

This command provides details about all objects and data that can be restored, similar to the `pg_restore -l` command in
PostgreSQL. It helps you inspect the contents of the dump before performing the actual restoration.

Parameters:

* `--format` — format of printing. Can be `text` or `json`.

To display metadata information about a dump, use the following command:

```shell
greenmask --config=config.yml show-dump dumpID
```

=== "Text output example"
```text
;
; Archive created at 2023-10-30 12:52:38 UTC
; dbname: demo
; TOC Entries: 17
; Compression: -1
; Dump Version: 15.4
; Format: DIRECTORY
; Integer: 4 bytes
; Offset: 8 bytes
; Dumped from database version: 15.4
; Dumped by pg_dump version: 15.4
;
;
; Selected TOC Entries:
;
3444; 0 0 ENCODING - ENCODING
3445; 0 0 STDSTRINGS - STDSTRINGS
3446; 0 0 SEARCHPATH - SEARCHPATH
3447; 1262 24970 DATABASE - demo postgres
3448; 0 0 DATABASE PROPERTIES - demo postgres
222; 1259 24999 TABLE bookings flights postgres
223; 1259 25005 SEQUENCE bookings flights_flight_id_seq postgres
3460; 0 0 SEQUENCE OWNED BY bookings flights_flight_id_seq postgres
3281; 2604 25030 DEFAULT bookings flights flight_id postgres
3462; 0 24999 TABLE DATA bookings flights postgres
3289; 2606 25044 CONSTRAINT bookings flights flights_flight_no_scheduled_departure_key postgres
3291; 2606 25046 CONSTRAINT bookings flights flights_pkey postgres
3287; 1259 42848 INDEX bookings flights_aircraft_code_status_idx postgres
3292; 1259 42847 INDEX bookings flights_status_aircraft_code_idx postgres
3293; 2606 25058 FK CONSTRAINT bookings flights flights_aircraft_code_fkey postgres
3294; 2606 25063 FK CONSTRAINT bookings flights flights_arrival_airport_fkey postgres
3295; 2606 25068 FK CONSTRAINT bookings flights flights_departure_airport_fkey postgres
```
=== "JSON output example"

    ```json linenums="1"
    {
      "startedAt": "2023-10-29T20:50:19.948017+02:00", // (1)
      "completedAt": "2023-10-29T20:50:22.19333+02:00", // (2)
      "originalSize": 4053842, // (3)
      "compressedSize": 686557, // (4)
      "transformers": [ // (5)
        {
          "Schema": "bookings", // (6)
          "Name": "flights", // (7)
          "Query": "", // (8)
          "Transformers": [ // (9)
            {
              "Name": "RandomDate", // (10)
              "Params": { // (11)
                "column": "c2NoZWR1bGVkX2RlcGFydHVyZQ==",
                "max": "MjAyMy0wMS0wMiAwMDowMDowMC4wKzAz",
                "min": "MjAyMy0wMS0wMSAwMDowMDowMC4wKzAz"
              }
            }
          ],
          "ColumnsTypeOverride": null // (12)
        }
      ],
      "header": { // (13)
        "creationDate": "2023-10-29T20:50:20+02:00",
        "dbName": "demo",
        "tocEntriesCount": 15,
        "dumpVersion": "16.0 (Homebrew)",
        "format": "TAR",
        "integer": 4,
        "offset": 8,
        "dumpedFrom": "16.0 (Debian 16.0-1.pgdg120+1)",
        "dumpedBy": "16.0 (Homebrew)",
        "tocFileSize": 8090,
        "compression": 0
      },
      "entries": [ // (14)
        {
          "dumpId": 3416,
          "databaseOid": 0,
          "objectOid": 0,
          "objectType": "ENCODING",
          "schema": "",
          "name": "ENCODING",
          "owner": "",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": null
        },
        {
          "dumpId": 3417,
          "databaseOid": 0,
          "objectOid": 0,
          "objectType": "STDSTRINGS",
          "schema": "",
          "name": "STDSTRINGS",
          "owner": "",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": null
        },
        {
          "dumpId": 3418,
          "databaseOid": 0,
          "objectOid": 0,
          "objectType": "SEARCHPATH",
          "schema": "",
          "name": "SEARCHPATH",
          "owner": "",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": null
        },
        {
          "dumpId": 3419,
          "databaseOid": 16384,
          "objectOid": 1262,
          "objectType": "DATABASE",
          "schema": "",
          "name": "demo",
          "owner": "postgres",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": null
        },
        {
          "dumpId": 3420,
          "databaseOid": 0,
          "objectOid": 0,
          "objectType": "DATABASE PROPERTIES",
          "schema": "",
          "name": "demo",
          "owner": "postgres",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": null
        },
        {
          "dumpId": 222,
          "databaseOid": 16414,
          "objectOid": 1259,
          "objectType": "TABLE",
          "schema": "bookings",
          "name": "flights",
          "owner": "postgres",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": null
        },
        {
          "dumpId": 223,
          "databaseOid": 16420,
          "objectOid": 1259,
          "objectType": "SEQUENCE",
          "schema": "bookings",
          "name": "flights_flight_id_seq",
          "owner": "postgres",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": [
            222
          ]
        },
        {
          "dumpId": 3432,
          "databaseOid": 0,
          "objectOid": 0,
          "objectType": "SEQUENCE OWNED BY",
          "schema": "bookings",
          "name": "flights_flight_id_seq",
          "owner": "postgres",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": [
            223
          ]
        },
        {
          "dumpId": 3254,
          "databaseOid": 16445,
          "objectOid": 2604,
          "objectType": "DEFAULT",
          "schema": "bookings",
          "name": "flights flight_id",
          "owner": "postgres",
          "section": "PreData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": [
            223,
            222
          ]
        },
        {
          "dumpId": 3434,
          "databaseOid": 16414,
          "objectOid": 0,
          "objectType": "TABLE DATA",
          "schema": "\"bookings\"",
          "name": "\"flights\"",
          "owner": "\"postgres\"",
          "section": "Data",
          "originalSize": 4045752,
          "compressedSize": 678467,
          "fileName": "3434.dat.gz",
          "dependencies": []
        },
        {
          "dumpId": 3261,
          "databaseOid": 16461,
          "objectOid": 2606,
          "objectType": "CONSTRAINT",
          "schema": "bookings",
          "name": "flights flights_flight_no_scheduled_departure_key",
          "owner": "postgres",
          "section": "PostData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": [
            222,
            222
          ]
        },
        {
          "dumpId": 3263,
          "databaseOid": 16463,
          "objectOid": 2606,
          "objectType": "CONSTRAINT",
          "schema": "bookings",
          "name": "flights flights_pkey",
          "owner": "postgres",
          "section": "PostData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": [
            222
          ]
        },
        {
          "dumpId": 3264,
          "databaseOid": 16477,
          "objectOid": 2606,
          "objectType": "FK CONSTRAINT",
          "schema": "bookings",
          "name": "flights flights_aircraft_code_fkey",
          "owner": "postgres",
          "section": "PostData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": [
            222
          ]
        },
        {
          "dumpId": 3265,
          "databaseOid": 16482,
          "objectOid": 2606,
          "objectType": "FK CONSTRAINT",
          "schema": "bookings",
          "name": "flights flights_arrival_airport_fkey",
          "owner": "postgres",
          "section": "PostData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": [
            222
          ]
        },
        {
          "dumpId": 3266,
          "databaseOid": 16487,
          "objectOid": 2606,
          "objectType": "FK CONSTRAINT",
          "schema": "bookings",
          "name": "flights flights_departure_airport_fkey",
          "owner": "postgres",
          "section": "PostData",
          "originalSize": 0,
          "compressedSize": 0,
          "fileName": "",
          "dependencies": [
            222
          ]
        }
      ]
    }
    ```
    { .annotate }

    1. The date when the backup has been initiated, also indicating the snapshot date.
    2. The date when the backup process was successfully completed.
    3. The original size of the backup in bytes.
    4. The size of the backup after compression in bytes.
    5. A list of tables that underwent transformation during the backup.
    6. The schema name of the table.
    7. The name of the table.
    8. Custom query override, if applicable.
    9. A list of transformers that were applied during the backup.
    10. The name of the transformer.
    11. The parameters provided for the transformer.
    12. A mapping of overridden column types.
    13. The header information in the table of contents file. This provides the same details as the `--format=text` output in the previous snippet.
    14. The list of restoration entries. This offers the same information as the `--format=text` output in the previous snippet.

!!! note

    The `json` format provides more detailed information compared to the `text` format. The `text` format is primarily used for backward compatibility and for generating a restoration list that can be used with `pg_restore -L listfile`. On the other hand, the `json` format provides comprehensive metadata about the dump, including information about the applied transformers and their parameters. The `json` format is especially useful for detailed dump introspection.
