package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v7"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	fields []physical.SchemaField
	table  string

	placeholderExprs []Expression
	client           *elasticsearch.Client
	stmt             string
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	placeholderValues := make([]interface{}, len(d.placeholderExprs))
	for i := range d.placeholderExprs {
		value, err := d.placeholderExprs[i].Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate pushed-down predicate placeholder expression: %w", err)
		}
		// TODO: Use internal function for this.
		placeholderValues[i] = value.ToRawGoValue()
	}

	params := map[string]interface{}{}
	sql := fmt.Sprintf(d.stmt, placeholderValues...)
	params["query"] = sql
	body, _ := json.Marshal(params)
	resp, err := d.client.SQL.Query(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("couldn't prepare statement '%s': %w", sql, err)
	}

	var data map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&data)

	var columns []map[string]interface{}
	json.Unmarshal(func() []byte {
		buf, _ := json.Marshal(data["columns"])
		return buf
	}(), &columns)

	var rows [][]interface{}
	json.Unmarshal(func() []byte {
		buf, _ := json.Marshal(data["rows"])
		return buf
	}(), &rows)

	log.Println(columns, rows)

	for i := 0; i < len(rows); i++ {
		recordValues := make([]octosql.Value, len(columns))
		for j := 0; j < len(columns); j++ {
			switch columns[j]["type"] {
			case "long":
				recordValues[j] = octosql.NewInt(int(rows[i][j].(float64)))
			case "float", "double":
				recordValues[j] = octosql.NewFloat(rows[i][j].(float64))
			case "bool", "boolean":
				recordValues[j] = octosql.NewBoolean(rows[i][j].(bool))
			case "text", "keyword":
				recordValues[j] = octosql.NewString(rows[i][j].(string))
			case "date", "datetime":
				value, err := time.Parse(time.RFC3339Nano, rows[i][j].(string))
				if err == nil {
					recordValues[j] = octosql.NewTime(value)
				} else {
					recordValues[j] = octosql.NewNull()
				}
			case nil:
				recordValues[j] = octosql.NewNull()
			default:
				log.Printf("unknown postgres value type, setting null: %T, %+v", rows[i][j], rows[i][j])
				recordValues[j] = octosql.NewNull()
			}
		}
		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(recordValues, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}
