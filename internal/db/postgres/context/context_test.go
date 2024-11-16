package context

import (
	"context"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	contextTestDb = `
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE users
(
    user_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL
);

CREATE TABLE orders
(
    order_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID REFERENCES users (user_id),
    order_date DATE NOT NULL
);


CREATE TABLE public."foo"
(
    id      uuid                   NOT NULL default uuid_generate_v4(),
    locale  text                   NOT NULL,
    subject character varying(100) NOT NULL,
    body    text                   NOT NULL
);

INSERT INTO users (username)
VALUES ('john_doe');
INSERT INTO users (username)
VALUES ('jane_smith');

INSERT INTO orders (user_id, order_date)
VALUES ((SELECT user_id FROM users WHERE username = 'john_doe'), '2024-10-31'),
       ((SELECT user_id FROM users WHERE username = 'jane_smith'), '2024-10-30');

insert into public."foo" (locale, subject, body)
values ('da-DK', 'Opsæt din konto hos {{coachName}}',
        ' Hej {{firstName}},\n\nSå er vi klar til at tage første skridt.\nTryk på følgende link for at få adgang til din side:\n[{{{accessLink}}}]({{{accessLink}}})\n\nMed venlig hilsen\n{{coachName}}'),
       ('da-DK', 'Kvittering på {{productName}}',
        'Hej {{firstName}},\n\nHermed en kvittering på din bestilling af {{productName}}\n\n- Total beløb: {{paymentAmount}}\n- Dato for køb: {{paymentDate}}\n\nBeløbet vil ikke blive trukket på din konto før den er fremsendt til dig.\n\nInden for et par dage vil jeg sende det din vej på samme e-mail, som du har modtaget denne kvittering på.\n\nRigtig god dag\n{{coachName}}'),
       ('en-US', 'Kvittering på {{productName}}',
        'Hej {{firstName}},\n\nHermed en kvittering på din bestilling af {{productName}}\n\n- Total beløb: {{paymentAmount}}\n- Dato for køb: {{paymentDate}}\n\nBeløbet vil ikke blive trukket på din konto før den er fremsendt til dig.\n\nInden for et par dage vil jeg sende det din vej på samme e-mail, som du har modtaget denne kvittering på.\n\nRigtig god dag\n{{coachName}}'),
       ('sv-SE', 'Kvittering på {{productName}}',
        'Hej {{firstName}},\n\nHermed en kvittering på din bestilling af {{productName}}\n\n- Total beløb: {{paymentAmount}}\n- Dato for køb: {{paymentDate}}\n\nBeløbet vil ikke blive trukket på din konto før den er fremsendt til dig.\n\nInden for et par dage vil jeg sende det din vej på samme e-mail, som du har modtaget denne kvittering på.\n\nRigtig god dag\n{{coachName}}'),
       ('fr-FR', 'Reçu de paiement pour le {{productName}}',
        'Bonjour {{firstName}},\n\nCeci est le reçu de paiement pour votre commande du  {{productName}}\n\n- Montant total: {{paymentAmount}}\n- Date d''achat: {{paymentDate}}\n\nVous ne serez pas facturé avant que le produit ne vous soit envoyé.\n\nDans quelques jours je vous l''enverrai sur cette adresse e-mail.\n\n\nBien cordialement\n{{coachName}}'),
       ('de-DE', 'Bestellung eines {{productName}}',
        'Hallo {{firstName}},\n\nAnbei findest du die Übersicht deiner Bestellung über einen {{productName}}\n\n- Gesamtbetrag: {{paymentAmount}}\n- Datum der Bestellung: {{paymentDate}}\n\nDir wird nichts in Rechnung gestellt, bevor du deinen Plan erhalten hast.\nIch werde dir in wenigen Tagen eine Bestätigung über diese E-Mail Adresse zukommen lassen. \n\nBeste Grüße, \n{{coachName}}'),
       ('es-MX', 'Recibo de {{productName}}',
        'Hola, {{firstName}}, \n\nEste es el recibo de tu pedido de un {{productName}} \n\n- Importe total: {{paymentAmount}} \n- Fecha de compra: {{paymentDate}} \n\nNo se te realizará el cargo hasta que se te envíe el producto.\n\nLo recibirás dentro de pocos días en este mismo correo electrónico. \n\nSaludos, \n{{coachName}}\n'),
       ('da-DK', 'Din kostplan',
        'Hej {{firstName}},\n\nDin kostplan er nu sammensat - du finder den vedhæftet som pdf.\n\nDu kan downloade og printe den, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå den via din smartphone, så du altid har den ved din side.\n\nJeg håber, at du bliver rigtig glad for planen. Følger du den, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte en kostplan hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('en-US', 'Din kostplan',
        'Hej {{firstName}},\n\nDin kostplan er nu sammensat - du finder den vedhæftet som pdf.\n\nDu kan downloade og printe den, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå den via din smartphone, så du altid har den ved din side.\n\nJeg håber, at du bliver rigtig glad for planen. Følger du den, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte en kostplan hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('sv-SE', 'Din kostplan',
        'Hej {{firstName}},\n\nDin kostplan er nu sammensat - du finder den vedhæftet som pdf.\n\nDu kan downloade og printe den, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå den via din smartphone, så du altid har den ved din side.\n\nJeg håber, at du bliver rigtig glad for planen. Følger du den, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte en kostplan hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('fr-FR', 'Votre programme alimentaire',
        'Bonjour {{firstName}},\n\nVotre programme alimentaire est maintenant prêt - il est joint à cet email en format PDF.\nVous pouvez le télécharger et l''imprimer puis l''accrocher facilement à votre réfrigérateur.\nVous pouvez également y accéder directement depuis votre portable.\n\nJ''espère vraiment que vous apprécierez de suivre ce plan. Si vous vous y tenez, je suis sûr que vous obtiendrez d''excellents résultats.\n\nMerci d''avoir passé votre commande.\n\nBien cordialement\n{{coachName}}\n'),
       ('de-DE', 'Dein Ernährungsplan',
        'Hallo {{firstName}},\n\nDein Ernährungsplan ist fertig und steht für dich bereit. Du findest ihn in der angehängten PDF. Du kannst das Dokument einfach herunterladen und ausdrucken, oder über dein Smartphone darauf zugreifen.\n\nIch hoffe sehr, dass du den Ernährungsplan genießt. Wenn du dich so gut es geht an den Plan hältst, bin ich zuversichtlich, dass du großartige Ergebnisse erzielen wirst\n\n\nVielen Dank für deine Bestellung.\n\nBeste Grüße, \n{{coachName}}'),
       ('es-MX', 'Tu plan alimenticio',
        'Hola, {{firstName}}, \n\nTu plan alimenticio está listo; está adjunto como PDF. Puedes descargarlo e imprimirlo y colgarlo fácilmente en tu refrigerador. También puedes acceder a él directamente desde tu smartphone. \n\nEspero que te guste el plan alimenticio. Si lo sigues, sin duda conseguirás grandes resultados. \n\nGracias por hacer tu pedido. \n\nSaludos, \n{{coachName}}'),
       ('da-DK', 'Dit træningsprogram',
        'Hej {{firstName}},\n\nDit træningsprogram er nu sammensat - du finder det vedhæftet som pdf.\n\nDu kan downloade og printe det, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå det via din smartphone, så du altid har det ved din side.\n\nJeg håber, at du bliver rigtig glad for programmet. Følger du det, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte et træningsprogram hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('en-US', 'Dit træningsprogram',
        'Hej {{firstName}},\n\nDit træningsprogram er nu sammensat - du finder det vedhæftet som pdf.\n\nDu kan downloade og printe det, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå det via din smartphone, så du altid har det ved din side.\n\nJeg håber, at du bliver rigtig glad for programmet. Følger du det, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte et træningsprogram hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('sv-SE', 'Dit træningsprogram',
        'Hej {{firstName}},\n\nDit træningsprogram er nu sammensat - du finder det vedhæftet som pdf.\n\nDu kan downloade og printe det, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå det via din smartphone, så du altid har det ved din side.\n\nJeg håber, at du bliver rigtig glad for programmet. Følger du det, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte et træningsprogram hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('fr-FR', 'Votre programme d''entraînement',
        'Bonjour {{firstName}},\n\nVotre programme d''entraînement est maintenant prêt - il est joint à cet email en format PDF.\nVous pouvez le télécharger et l''imprimer afin de le transporter avec vous.\nVous pouvez également y accéder directement depuis votre portable.\n\nJ''espère vraiment que vous apprécierez de suivre ce plan. Si vous vous y tenez, je suis sûr que vous obtiendrez d''excellents résultats.\n\nMerci d''avoir passé votre commande.\n\nBien cordialement\n{{coachName}})');
`
)

func TestNewRuntimeContext(t *testing.T) {
	ctx := context.Background()
	// Start the PostgreSQL container
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup() // Ensure the container is terminated after the test

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, contextTestDb))
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "users",
				Transformers: []*domains.TransformerConfig{
					{
						Name:               transformers.RandomUuidTransformerName,
						ApplyForReferences: true,
						Params: toolkit.StaticParameters{
							"column": toolkit.ParamsValue("user_id"),
							"engine": toolkit.ParamsValue("hash"),
						},
					},
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())
}

func TestNewRuntimeContext_regression_244(t *testing.T) {
	// This test is a regression test for https://github.com/GreenmaskIO/greenmask/issues/244
	// The problem was that the table graph used a shared tables list, which was later sorted by the table size scoring
	// function. This means that the graph tables must not be sorted by the size scoring function.
	ctx := context.Background()
	// Start the PostgreSQL container
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup() // Ensure the container is terminated after the test

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, contextTestDb))
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "users",
				Transformers: []*domains.TransformerConfig{
					{
						Name:               transformers.RandomUuidTransformerName,
						ApplyForReferences: true,
						Params: toolkit.StaticParameters{
							"column": toolkit.ParamsValue("user_id"),
							"engine": toolkit.ParamsValue("hash"),
						},
					},
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())

	// Check that tables are sorted by oid in graph as defined in buildTableSearchQuery function
	tablesOids := make([]toolkit.Oid, 0, len(rc.DataSectionObjects))
	for _, table := range rc.DataSectionObjects {
		tab, ok := table.(*entries.Table)
		if !ok {
			continue
		}
		tablesOids = append(tablesOids, tab.Oid)
	}
	slices.Sort(tablesOids)

	for posInContext, table := range rc.Graph.GetTables() {
		expectedPos := slices.Index(tablesOids, table.Oid)
		assert.Equalf(t, expectedPos, posInContext,
			"Expected table %s to be at position %d, but it is at %d", table.Name, expectedPos, posInContext,
		)
	}

	// Validate inherited transformers
	expectedTablesWithTransformer := map[string]int{
		"users":  1,
		"orders": 1,
		"foo":    0,
	}

	for _, table := range rc.DataSectionObjects {
		tab, ok := table.(*entries.Table)
		if !ok {
			continue
		}
		if _, ok := expectedTablesWithTransformer[tab.Name]; ok {
			assert.Equalf(t, expectedTablesWithTransformer[tab.Name], len(tab.TransformersContext), "Table %s", tab.Name)
		} else {
			assert.Empty(t, tab.TransformersContext, "Table %s", tab.Name)
		}
	}
}

func TestNewRuntimeContext_regression_247(t *testing.T) {
	// This test is a regression test for https://github.com/GreenmaskIO/greenmask/issues/247
	// It validates that subset conditions are correctly applied to the query
	ctx := context.Background()
	// Start the PostgreSQL container
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup() // Ensure the container is terminated after the test

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, contextTestDb))
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "users",
				SubsetConds: []string{
					"public.users.user_id = '62c8c546-2420-4ca6-9961-d2cce26f7cb2'",
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())

	expectedTablesWithSubsetQuery := map[string]string{
		"users":  "SELECT \"public\".\"users\".* FROM \"public\".\"users\"   WHERE ( ( public.users.user_id = '62c8c546-2420-4ca6-9961-d2cce26f7cb2' ) )",
		"orders": "SELECT \"public\".\"orders\".* FROM \"public\".\"orders\"  LEFT JOIN \"public\".\"users\" ON \"public\".\"orders\".\"user_id\" = \"public\".\"users\".\"user_id\" AND ( public.users.user_id = '62c8c546-2420-4ca6-9961-d2cce26f7cb2' ) WHERE ( ((\"public\".\"orders\".\"user_id\" IS NULL OR \"public\".\"users\".\"user_id\" IS NOT NULL)) )",
		"foo":    "",
	}

	for _, table := range rc.DataSectionObjects {
		tab, ok := table.(*entries.Table)
		if !ok {
			continue
		}
		assert.Equalf(t, expectedTablesWithSubsetQuery[tab.Name], tab.Query, "Table %s", tab.Name)
	}
}
