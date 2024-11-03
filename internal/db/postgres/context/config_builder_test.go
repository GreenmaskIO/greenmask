package context

import (
	"context"
	"fmt"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/subset"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	testContainerPgVersion   = 17
	testContainerPort        = "5432"
	testContainerDatabase    = "testdb"
	testContainerUser        = "testuser"
	testContainerPassword    = "testpassword"
	testContainerImage       = "postgres:17"
	testContainerExposedPort = "5432/tcp"
)

func Test_isTransformerAllowedToApplyForReferences(t *testing.T) {
	r := utils.DefaultTransformerRegistry

	t.Run("RandomInt and hash engine", func(t *testing.T) {
		cfg := &domains.TransformerConfig{
			Name:               transformers.RandomIntTransformerName,
			ApplyForReferences: true,
			Params: toolkit.StaticParameters{
				"column": toolkit.ParamsValue("id"),
				"engine": toolkit.ParamsValue("hash"),
			},
		}
		ok, w := isTransformerAllowedToApplyForReferences(cfg, r)
		require.Empty(t, w)
		require.True(t, ok)
	})

	t.Run("RandomInt and without hash engine", func(t *testing.T) {
		cfg := &domains.TransformerConfig{
			Name:               transformers.RandomIntTransformerName,
			ApplyForReferences: true,
			Params: toolkit.StaticParameters{
				"column": toolkit.ParamsValue("id"),
				"engine": toolkit.ParamsValue("random"),
			},
		}
		ok, w := isTransformerAllowedToApplyForReferences(cfg, r)
		require.NotEmpty(t, w)
		require.False(t, ok)
	})

	t.Run("Template", func(t *testing.T) {
		cfg := &domains.TransformerConfig{
			Name:               transformers.TemplateTransformerName,
			ApplyForReferences: true,
			Params: toolkit.StaticParameters{
				"column": toolkit.ParamsValue("id"),
			},
		}
		ok, w := isTransformerAllowedToApplyForReferences(cfg, r)
		require.NotEmpty(t, w)
		require.False(t, ok)
	})

	t.Run("Unknown name", func(t *testing.T) {
		cfg := &domains.TransformerConfig{
			Name:               "unknown",
			ApplyForReferences: true,
			Params: toolkit.StaticParameters{
				"column": toolkit.ParamsValue("id"),
			},
		}
		ok, w := isTransformerAllowedToApplyForReferences(cfg, r)
		require.NotEmpty(t, w)
		require.False(t, ok)
	})
}

func Test_runPostgresContainer(t *testing.T) {
	ctx := context.Background()
	// Start the PostgreSQL container
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup() // Ensure the container is terminated after the test

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
}

func Test_validateAndBuildEntriesConfig(t *testing.T) {
	ctx := context.Background()
	// Start the PostgreSQL container
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup() // Ensure the container is terminated after the test

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con))

	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck

	pgVer := testContainerPgVersion * 10000 // 170000
	opt := &pgdump.Options{}
	typeMap := tx.Conn().TypeMap()
	types, err := buildTypeMap(ctx, tx, typeMap)
	require.NoError(t, err)
	t.Run("Apply for one transformer", func(t *testing.T) {

		tables, _, _, err := getDumpObjects(ctx, pgVer, tx, opt)
		require.NoError(t, err)
		graph, err := subset.NewGraph(ctx, tx, tables, nil)
		require.NoError(t, err)

		cfg := &domains.Dump{
			Transformation: []*domains.Table{
				{
					Schema: "public",
					Name:   "tablea",
					Transformers: []*domains.TransformerConfig{
						{
							Name: transformers.RandomIntTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("id1"),
								"engine": toolkit.ParamsValue("hash"),
							},
						},
						{
							Name: transformers.RandomIntTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("id2"),
								"engine": toolkit.ParamsValue("hash"),
							},
						},
					},
				},
			},
		}
		vw, err := validateAndBuildEntriesConfig(
			ctx, tx, tables, typeMap, cfg,
			utils.DefaultTransformerRegistry, pgVer, types, graph,
		)
		require.NoError(t, err)
		require.False(t, vw.IsFatal())

		expectedTablesWithTransformer := map[string]int{
			"tablea": 2,
		}

		for _, table := range tables {
			if _, ok := expectedTablesWithTransformer[table.Name]; ok {
				assert.Equalf(t, expectedTablesWithTransformer[table.Name], len(table.TransformersContext), "Table %s", table.Name)
			} else {
				assert.Empty(t, table.TransformersContext, "Table %s", table.Name)
			}
		}
	})

	t.Run("ApplyForReferences is true", func(t *testing.T) {
		tables, _, _, err := getDumpObjects(ctx, pgVer, tx, opt)
		require.NoError(t, err)
		graph, err := subset.NewGraph(ctx, tx, tables, nil)
		require.NoError(t, err)

		cfg := &domains.Dump{
			Transformation: []*domains.Table{
				{
					Schema: "public",
					Name:   "tablea",
					Transformers: []*domains.TransformerConfig{
						{
							ApplyForReferences: true,
							Name:               transformers.RandomIntTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("id1"),
								"engine": toolkit.ParamsValue("hash"),
							},
						},
						{
							ApplyForReferences: true,
							Name:               transformers.RandomIntTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("id2"),
								"engine": toolkit.ParamsValue("hash"),
							},
						},
					},
				},
			},
		}
		vw, err := validateAndBuildEntriesConfig(
			ctx, tx, tables, typeMap, cfg,
			utils.DefaultTransformerRegistry, pgVer, types, graph,
		)
		require.NoError(t, err)
		require.False(t, vw.IsFatal())

		expectedTablesWithTransformer := map[string]int{
			"tablea": 2,
			"tableb": 2,
			"tablec": 2,
		}

		for _, table := range tables {
			if _, ok := expectedTablesWithTransformer[table.Name]; ok {
				assert.Equalf(t, expectedTablesWithTransformer[table.Name], len(table.TransformersContext), "Table %s", table.Name)
			} else {
				assert.Empty(t, table.TransformersContext, "Table %s", table.Name)
			}
		}
	})

	t.Run("ApplyForReferences is true and ref transformer created manually", func(t *testing.T) {
		tables, _, _, err := getDumpObjects(ctx, pgVer, tx, opt)
		require.NoError(t, err)
		graph, err := subset.NewGraph(ctx, tx, tables, nil)
		require.NoError(t, err)

		cfg := &domains.Dump{
			Transformation: []*domains.Table{
				{
					Schema: "public",
					Name:   "tablea",
					Transformers: []*domains.TransformerConfig{
						{
							ApplyForReferences: true,
							Name:               transformers.RandomIntTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("id1"),
								"engine": toolkit.ParamsValue("hash"),
							},
						},
						{
							ApplyForReferences: true,
							Name:               transformers.RandomIntTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("id2"),
								"engine": toolkit.ParamsValue("hash"),
							},
						},
					},
				},
				{
					Schema: "public",
					Name:   "tablec",
					Transformers: []*domains.TransformerConfig{
						{
							ApplyForReferences: true,
							Name:               transformers.RandomIntTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("id1"),
								"engine": toolkit.ParamsValue("hash"),
							},
						},
						{
							ApplyForReferences: true,
							Name:               transformers.RandomIntTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("id2"),
								"engine": toolkit.ParamsValue("hash"),
							},
						},
					},
				},
			},
		}
		vw, err := validateAndBuildEntriesConfig(
			ctx, tx, tables, typeMap, cfg,
			utils.DefaultTransformerRegistry, pgVer, types, graph,
		)
		require.NoError(t, err)
		require.False(t, vw.IsFatal())

		expectedTablesWithTransformer := map[string]int{
			"tablea": 2,
			"tableb": 2,
			"tablec": 2,
			"tabled": 2,
		}

		for _, table := range tables {
			if _, ok := expectedTablesWithTransformer[table.Name]; ok {
				assert.Equalf(t, expectedTablesWithTransformer[table.Name], len(table.TransformersContext), "Table %s", table.Name)
			} else {
				assert.Empty(t, table.TransformersContext, "Table %s", table.Name)
			}
		}
	})

	t.Run("ApplyForInherited", func(t *testing.T) {
		tables, _, _, err := getDumpObjects(ctx, pgVer, tx, opt)
		require.NoError(t, err)
		graph, err := subset.NewGraph(ctx, tx, tables, nil)
		require.NoError(t, err)

		cfg := &domains.Dump{
			Transformation: []*domains.Table{
				{
					Schema:            "public",
					Name:              "sales",
					ApplyForInherited: true,
					Transformers: []*domains.TransformerConfig{
						{
							Name: transformers.RandomDateTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("sale_date"),
								"engine": toolkit.ParamsValue("random"),
								"min":    toolkit.ParamsValue("2000-01-01"),
								"max":    toolkit.ParamsValue("2005-01-01"),
							},
						},
					},
				},
			},
		}
		vw, err := validateAndBuildEntriesConfig(
			ctx, tx, tables, typeMap, cfg,
			utils.DefaultTransformerRegistry, pgVer, types, graph,
		)
		require.NoError(t, err)
		require.False(t, vw.IsFatal())

		expectedTablesWithTransformer := map[string]int{
			"sales_2022_jan": 1,
			"sales_2022_feb": 1,
			"sales_2022_mar": 1,
			"sales_2023_jan": 1,
			"sales_2023_feb": 1,
			"sales_2023_mar": 1,
		}

		for _, table := range tables {
			if _, ok := expectedTablesWithTransformer[table.Name]; ok {
				assert.Equalf(t, expectedTablesWithTransformer[table.Name], len(table.TransformersContext), "Table %s", table.Name)
			} else {
				assert.Empty(t, table.TransformersContext, "Table %s", table.Name)
			}
		}
	})

	t.Run("ApplyForInherited with manually defined on part", func(t *testing.T) {
		tables, _, _, err := getDumpObjects(ctx, pgVer, tx, opt)
		require.NoError(t, err)
		graph, err := subset.NewGraph(ctx, tx, tables, nil)
		require.NoError(t, err)

		cfg := &domains.Dump{
			Transformation: []*domains.Table{
				{
					Schema:            "public",
					Name:              "sales",
					ApplyForInherited: true,
					Transformers: []*domains.TransformerConfig{
						{
							Name: transformers.RandomDateTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("sale_date"),
								"engine": toolkit.ParamsValue("random"),
								"min":    toolkit.ParamsValue("2000-01-01"),
								"max":    toolkit.ParamsValue("2005-01-01"),
							},
						},
					},
				},
				{
					Schema: "public",
					Name:   "sales_2022_feb",
					Transformers: []*domains.TransformerConfig{
						{
							Name: transformers.RandomDateTransformerName,
							Params: toolkit.StaticParameters{
								"column": toolkit.ParamsValue("sale_date"),
								"engine": toolkit.ParamsValue("random"),
								"min":    toolkit.ParamsValue("2001-01-01"),
								"max":    toolkit.ParamsValue("2005-01-01"),
							},
						},
					},
				},
			},
		}
		vw, err := validateAndBuildEntriesConfig(
			ctx, tx, tables, typeMap, cfg,
			utils.DefaultTransformerRegistry, pgVer, types, graph,
		)
		require.NoError(t, err)
		require.False(t, vw.IsFatal())

		expectedTablesWithTransformer := map[string]int{
			"sales_2022_jan": 1,
			"sales_2022_feb": 2,
			"sales_2022_mar": 1,
			"sales_2023_jan": 1,
			"sales_2023_feb": 1,
			"sales_2023_mar": 1,
		}

		for _, table := range tables {
			if _, ok := expectedTablesWithTransformer[table.Name]; ok {
				assert.Equalf(t, expectedTablesWithTransformer[table.Name], len(table.TransformersContext), "Table %s", table.Name)
			} else {
				assert.Empty(t, table.TransformersContext, "Table %s", table.Name)
			}
		}
	})
}

// runPostgresContainer starts a PostgreSQL container and returns the connection string
func runPostgresContainer(ctx context.Context) (string, func(), error) {
	req := testcontainers.ContainerRequest{
		Image:        testContainerImage,                 // Specify the PostgreSQL image
		ExposedPorts: []string{testContainerExposedPort}, // Expose the PostgreSQL port
		Env: map[string]string{
			"POSTGRES_USER":     testContainerUser,
			"POSTGRES_PASSWORD": testContainerPassword,
			"POSTGRES_DB":       testContainerDatabase,
		},
		WaitingFor: wait.ForSQL(testContainerExposedPort, "pgx", func(host string, port nat.Port) string {
			return fmt.Sprintf(
				"postgres://%s:%s@%s:%s/%s?sslmode=disable",
				testContainerUser, testContainerPassword, host, port.Port(), testContainerDatabase,
			)
		}),
	}

	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to start PostgreSQL container: %w", err)
	}

	// Get the host and port for connecting to the container
	host, err := postgresContainer.Host(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get container host: %w", err)
	}
	port, err := postgresContainer.MappedPort(ctx, testContainerPort)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get container port: %w", err)
	}

	// Create the connection string
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		testContainerUser, testContainerPassword, host, port.Port(), testContainerDatabase,
	)

	// Return the connection string and cleanup function
	return connStr, func() {
		_ = postgresContainer.Terminate(ctx)
	}, nil
}

func initTables(ctx context.Context, con *pgx.Conn) error {
	sqlScript := `
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- tables with end-to-end FK/PK relationships

	-- Step 1: Create TableA with a composite primary key
CREATE TABLE TableA
(
    id1  INT,
    id2  INT,
    data VARCHAR(50),
    PRIMARY KEY (id1, id2)
);

-- Step 2: Create TableB with a composite primary key and a foreign key reference to TableA
CREATE TABLE TableB
(
    id1    INT,
    id2    INT,
    detail VARCHAR(50),
    PRIMARY KEY (id1, id2),
    FOREIGN KEY (id1, id2) REFERENCES TableA (id1, id2) ON DELETE CASCADE
);

-- Step 3: Create TableC with a composite primary key and a foreign key reference to TableB
CREATE TABLE TableC
(
    id1         INT,
    id2         INT,
    description VARCHAR(50),
    PRIMARY KEY (id1, id2),
    FOREIGN KEY (id1, id2) REFERENCES TableB (id1, id2) ON DELETE CASCADE
);

-- Step 4: Insert sample data into TableA
INSERT INTO TableA (id1, id2, data)
VALUES (1, 1, 'Data A1'),
       (2, 1, 'Data A2'),
       (3, 1, 'Data A3');

-- Step 5: Insert sample data into TableB, referencing TableA
INSERT INTO TableB (id1, id2, detail)
VALUES (1, 1, 'Detail B1'),
       (2, 1, 'Detail B2'),
       (3, 1, 'Detail B3');

-- Step 6: Insert sample data into TableC, referencing TableB
INSERT INTO TableC (id1, id2, description)
VALUES (1, 1, 'Description C1'),
       (2, 1, 'Description C2'),
       (3, 1, 'Description C3');


-- Step 1: Create TableD with a serial primary key and a composite foreign key reference to TableC
CREATE TABLE TableD
(
    id           SERIAL PRIMARY KEY, -- Unique identifier for TableD
    id1          INT,
    id2          INT,
    extra_detail VARCHAR(50),
    UNIQUE (id1, id2),               -- Composite unique constraint for id1 and id2
    FOREIGN KEY (id1, id2) REFERENCES TableC (id1, id2) ON DELETE CASCADE
);

-- Step 2: Create TableE with a reference to TableD based on the primary key id
CREATE TABLE TableE
(
    id              SERIAL PRIMARY KEY, -- Unique identifier for TableE
    tabled_id       INT,
    additional_info VARCHAR(50),
    FOREIGN KEY (tabled_id) REFERENCES TableD (id) ON DELETE CASCADE
);

-- Step 3: Insert sample data into TableD referencing TableC
INSERT INTO TableD (id1, id2, extra_detail)
VALUES (1, 1, 'Extra Detail D1'),
       (2, 1, 'Extra Detail D2'),
       (3, 1, 'Extra Detail D3');

-- Step 4: Insert sample data into TableE referencing TableD
-- Use the 'id' from TableD for the 'tabled_id' in TableE
INSERT INTO TableE (tabled_id, additional_info)
VALUES (1, 'Additional Info E1'),
       (2, 'Additional Info E2'),
       (3, 'Additional Info E3');

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE sales
(
    sale_id   SERIAL         NOT NULL,
    sale_date DATE           NOT NULL,
    amount    NUMERIC(10, 2) NOT NULL
) PARTITION BY RANGE (EXTRACT(YEAR FROM sale_date));

-- Step 2: Create first-level partitions by year
CREATE TABLE sales_2022 PARTITION OF sales
    FOR VALUES FROM (2022) TO (2023)
    PARTITION BY LIST (EXTRACT(MONTH FROM sale_date));

CREATE TABLE sales_2023 PARTITION OF sales
    FOR VALUES FROM (2023) TO (2024)
    PARTITION BY LIST (EXTRACT(MONTH FROM sale_date));

-- Step 3: Create second-level partitions by month for each year, adding PRIMARY KEY on each partition

-- Monthly partitions for 2022
CREATE TABLE sales_2022_jan PARTITION OF sales_2022 FOR VALUES IN (1)
    WITH (fillfactor = 70);
CREATE TABLE sales_2022_feb PARTITION OF sales_2022 FOR VALUES IN (2);
CREATE TABLE sales_2022_mar PARTITION OF sales_2022 FOR VALUES IN (3);
-- Continue adding monthly partitions for 2022...

-- Monthly partitions for 2023
CREATE TABLE sales_2023_jan PARTITION OF sales_2023 FOR VALUES IN (1);
CREATE TABLE sales_2023_feb PARTITION OF sales_2023 FOR VALUES IN (2);
CREATE TABLE sales_2023_mar PARTITION OF sales_2023 FOR VALUES IN (3);
-- Continue adding monthly partitions for 2023...

-- Step 4: Insert sample data
INSERT INTO sales (sale_date, amount)
VALUES ('2022-01-15', 100.00);
INSERT INTO sales (sale_date, amount)
VALUES ('2022-02-20', 150.00);
INSERT INTO sales (sale_date, amount)
VALUES ('2023-03-10', 200.00);
`
	_, err := con.Exec(ctx, sqlScript)
	return err
}
