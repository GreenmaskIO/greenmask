## list-dumps command

The `list-dumps` command provides a list of all dumps stored in the storage. The list includes the following attributes:

* `ID` — the unique identifier of the dump, used for operations like `restore`, `delete`, and `show-dump`
* `DATE` — the date when the snapshot was created
* `DATABASE` — the name of the database associated with the dump
* `SIZE` — the original size of the dump
* `COMPRESSED SIZE` — the size of the dump after compression
* `DURATION` — the duration of the dump procedure
* `TRANSFORMED` — indicates whether the dump has been transformed
* `STATUS` — the status of the dump, which can be one of the following:
    * `done` — the dump was completed successfully
    * `unknown` or `failed` — the dump might be in progress or failed. Failed dumps are not deleted automatically.

Example of `list-dumps` output:
![list_dumps_screen.png](../assets/list_dumps_screen.png)



