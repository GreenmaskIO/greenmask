# Leading Zeros Bug Reproduction

This directory contains a reproduction case for issue #394 - VARCHAR columns
containing numeric-looking strings (like GTINs with leading zeros) lose their
leading zeros during Greenmask dump/restore.

## The Bug

When a VARCHAR column contains a value like `00001402417161` (a GTIN-14 with
leading zeros), Greenmask's dump process can strip the leading zeros, resulting
in `1402417161`.

This happens when:
1. A transformer (like Template) reads the column value via `GetColumnValue`
2. The value passes through the encode/decode cycle
3. Leading zeros are lost because the numeric-looking string is interpreted

## Running the Reproduction

```bash
./reproduce.sh
```

This script will:
1. Start a PostgreSQL container
2. Create a table with VARCHAR column containing GTINs with leading zeros
3. Run Greenmask dump with a simple pass-through Template transformer
4. Show the before/after values to demonstrate the bug

## Expected vs Actual

**Expected:** VARCHAR value `00001402417161` should remain `00001402417161`

**Actual (bug):** VARCHAR value becomes `1402417161` (leading zeros stripped)

## Fix

The fix in this PR ensures text-typed columns (varchar, text, char, name)
preserve their exact content by:
1. Returning raw bytes as string in `GetColumnValueByIdx` for text types
2. Properly handling text types in `encodeValue`
