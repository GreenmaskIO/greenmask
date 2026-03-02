# Subset system

## Notes

### Common

The subset system works from the top down. Meaning we start from the top vertex (the one that is dumped right now)
and then we go down.

### Nullable edges

We can see that the decision for record when joining sales.store table is made by the next where condition:

```sql

    (
        "sales"."customer"."storeid" IS NULL 				-- if left table is null ok - we proceed
        OR "sales"."store"."businessentityid" IS NOT NULL 	-- if left table is not null then the right table must be not null too.
    )

```

* Check if the FK table (the left table) is NULL - if is null it's allowed into selection unless
  id does not have the subset condition for this table.
* If FK table (the left table) is NOT NULL then the value of joined PK column value (of the right table)
  can't be NULL - meaning it must be in the selection.


The total query example:

```sql
SELECT "sales"."salesorderheadersalesreason".*
FROM "sales"."salesorderheadersalesreason"
         INNER JOIN "sales"."salesorderheader"
                    ON "sales"."salesorderheadersalesreason"."salesorderid" = "sales"."salesorderheader"."salesorderid"
         INNER JOIN "sales"."customer" ON "sales"."salesorderheader"."customerid" = "sales"."customer"."customerid"
         LEFT JOIN "sales"."store" ON "sales"."customer"."storeid" = "sales"."store"."businessentityid"
         LEFT JOIN "sales"."salesperson" ON "sales"."store"."salespersonid" = "sales"."salesperson"."businessentityid"
         LEFT JOIN "humanresources"."employee"
                   ON "sales"."salesperson"."businessentityid" = "humanresources"."employee"."businessentityid" AND
                      (humanresources.employee.businessentityid = 1)
WHERE ((("sales"."customer"."storeid" IS NULL OR "sales"."store"."businessentityid" IS NOT NULL)))
  AND ((("sales"."store"."salespersonid" IS NULL OR "sales"."salesperson"."businessentityid" IS NOT NULL)))
  AND ((("sales"."salesperson"."businessentityid" IS NULL OR
         "humanresources"."employee"."businessentityid" IS NOT NULL)))
  AND (("sales"."salesorderheader"."salespersonid" IS NULL) OR
       (("sales"."salesorderheader"."salespersonid") IN (SELECT "sales"."salesperson"."businessentityid"
                                                         FROM "sales"."salesperson"
                                                                  INNER JOIN "humanresources"."employee"
                                                                             ON "sales"."salesperson"."businessentityid" =
                                                                                "humanresources"."employee"."businessentityid" AND
                                                                                (humanresources.employee.businessentityid = 1)
                                                         WHERE TRUE)))
```

#### The implementation idea

We have the main object called `Query` it may contain the next Nodes:
* Select
* From
* Joins
* Where

The `Where` node may contain
* `Conditions`

Each `Condition` may be represented by 
* Direct condition

### Cycles

TODO: Implement cycles query builder.
