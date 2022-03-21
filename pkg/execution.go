package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	placeholderValues := make([]any, len(d.placeholderExprs))
	for i := range d.placeholderExprs {
		value, err := d.placeholderExprs[i].Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate pushed-down predicate placeholder expression: %w", err)
		}
		// TODO: Use internal function for this.
		placeholderValues[i] = value.ToRawGoValue()
	}

	params := map[string]any{}
	sql := fmt.Sprintf(d.stmt, placeholderValues...)
	params["query"] = sql
	body, _ := json.Marshal(params)
	resp, err := d.client.SQL.Query(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("couldn't prepare statement '%s': %w", sql, err)
	}

	if resp.StatusCode != http.StatusOK {
		var data ErrorResponse
		err = json.NewDecoder(resp.Body).Decode(&data)
		if err != nil {
			panic(err)
		}
		return data.GetError()
	}

	var data SQLQueryResponse
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return fmt.Errorf("couldn't parse response: %w", err)
	}

	for i := 0; i < len(data.Rows); i++ {
		recordValues := make([]octosql.Value, len(data.Columns))
		for j := 0; j < len(data.Columns); j++ {
			switch data.Columns[j].Type {
			case "long":
				recordValues[j] = octosql.NewInt(int(data.Rows[i][j].(float64)))
			case "float", "double":
				recordValues[j] = octosql.NewFloat(data.Rows[i][j].(float64))
			case "bool", "boolean":
				recordValues[j] = octosql.NewBoolean(data.Rows[i][j].(bool))
			case "text", "keyword":
				recordValues[j] = octosql.NewString(data.Rows[i][j].(string))
			case "date", "datetime":
				value, err := time.Parse(time.RFC3339Nano, data.Rows[i][j].(string))
				if err == nil {
					recordValues[j] = octosql.NewTime(value)
				} else {
					recordValues[j] = octosql.NewNull()
				}
			case "":
				recordValues[j] = octosql.NewNull()
			default:
				log.Printf("unknown postgres value type, setting null: %T, %+v", data.Rows[i][j], data.Rows[i][j])
				recordValues[j] = octosql.NewNull()
			}
		}
		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(recordValues, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}
