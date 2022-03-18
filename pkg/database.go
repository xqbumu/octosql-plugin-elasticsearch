package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type Config struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (c *Config) Validate() error {
	return nil
}

type testLogger struct {
}

func (t *testLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func connect(config *Config) (*elasticsearch.Client, error) {
	var options elasticsearch.Config

	options.Addresses = strings.Split(config.URL, ",")
	options.Username = config.Username
	options.Password = config.Password

	if os.Getenv("OCTOSQL_ELASTICSEARCH_QUERY_LOGGING") == "1" {
		options.EnableDebugLogger = true
	}

	client, err := elasticsearch.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("couldn't open database: %w", err)
	}

	// Ping the Elasticsearch server to get e.g. the version number
	info, err := client.Info()
	if err != nil {
		panic(err)
	}
	log.Printf("Elasticsearch returned with code %d and info:\n%s\n", info.StatusCode, info.Body)

	return client, nil
}

func Creator(ctx context.Context, configUntyped plugins.ConfigDecoder) (physical.Database, error) {
	var cfg Config
	if err := configUntyped.Decode(&cfg); err != nil {
		return nil, err
	}
	return &Database{
		Config: &cfg,
	}, nil
}

type Database struct {
	Config *Config
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	client, err := connect(d.Config)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't connect to database: %w", err)
	}

	resp, err := client.Indices.GetMapping(
		func(request *esapi.IndicesGetMappingRequest) {
			request.Index = []string{name}
		},
	)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't describe table: %w", err)
	}

	var data map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&data)
	index := data[name].(map[string]interface{})
	mappings := index["mappings"].(map[string]interface{})
	properties := mappings["properties"].(map[string]interface{})

	type FieldMeta struct {
		Type        string `json:"type"`
		Format      string `json:"format"`
		IgnoreAbove int    `json:"ignore_above"`
	}
	body, _ := json.Marshal(properties)
	fieldRaws := map[string]FieldMeta{}
	err = json.Unmarshal(body, &fieldRaws)
	if err != nil {
		panic(err)
	}

	var descriptions [][]string
	for fieldName, field := range fieldRaws {
		if len(field.Type) == 0 {
			fieldName = strings.TrimLeft(fieldName, "_")
			switch fieldName {
			case "id":
				descriptions = append(descriptions, []string{fieldName, "keyword", "No"})
			default:
				continue
			}
		} else {
			descriptions = append(descriptions, []string{fieldName, field.Type, "No"})
		}
	}
	if len(descriptions) == 0 {
		return nil, physical.Schema{}, fmt.Errorf("table %s does not exist", name)
	}

	fields := make([]physical.SchemaField, 0, len(descriptions))
	for i := range descriptions {
		t, ok := getOctoSQLType(descriptions[i][1])
		if !ok {
			continue
		}
		if descriptions[i][2] == "YES" {
			t = octosql.TypeSum(t, octosql.Null)
		}
		fields = append(fields, physical.SchemaField{
			Name: descriptions[i][0],
			Type: t,
		})
	}

	return &impl{
			config: d.Config,
			table:  name,
		},
		physical.Schema{
			Fields:    fields,
			TimeField: -1,
		},
		nil
}

func getOctoSQLType(typename string) (octosql.Type, bool) {
	if strings.HasPrefix(typename, "_") {
		elementType, ok := getOctoSQLType(typename[1:])
		if !ok {
			return octosql.Type{}, false
		}

		return octosql.Type{
			TypeID: octosql.TypeIDList,
			List: struct {
				Element *octosql.Type
			}{Element: &elementType},
		}, true
	}

	switch typename {
	case "long":
		return octosql.Int, true
	case "text", "keyword":
		return octosql.String, true
	case "float", "double":
		return octosql.Float, true
	case "bool", "boolean":
		return octosql.Boolean, true
	case "date":
		return octosql.Time, true
	case "ip":
		// TODO: Handle me better.
		return octosql.String, true
	default:
		log.Printf("unsupported postgres field type '%s' - ignoring column", typename)
		return octosql.Null, false

		// TODO: Support more types.
	}
}
