The `RandomSentence` transformer is designed to populate specified database columns with random sentences. Ideal for simulating natural language text for user comments, testing NLP systems, or anonymizing textual data in databases.

## Parameters

| Name       | Description                                           | Default | Required | Supported DB types |
|------------|-------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected                |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved   | `false` | No       | -                  |

## Description

The `RandomSentence` transformer employs complex text generation algorithms or libraries to generate random sentences, injecting them into a designated database column without the need for specifying sentence length. This flexibility ensures the creation of varied and plausible text for a wide range of applications.

## Example: Populate random sentences for the `comments` table

This example shows how to configure the `RandomSentence` transformer to populate the `comment` column in the `comments` table with random sentences. It is a straightforward method for simulating diverse user-generated content.

```yaml title="RandomSentence transformer example"
- schema: "public"
  name: "comments"
  transformers:
    - name: "RandomSentence"
      params:
        column: "comment"
        keep_null: false
```

In this configuration, the `comment` column will be updated with random sentences for each entry, replacing any existing non-NULL values. If `keep_null` is set to `true`, existing NULL values in the column will be preserved, maintaining the integrity of records where comments are not applicable.
