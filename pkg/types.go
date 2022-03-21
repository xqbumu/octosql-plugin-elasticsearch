package pkg

import (
	"fmt"
)

type (
	Error struct {
		RootCause    []*Error `json:"root_cause"`
		Type         string   `json:"type"`
		Reason       string   `json:"reason"`
		ResourceType string   `json:"resource.type"`
		ResourceID   string   `json:"resource.id"`
		IndexUUID    string   `json:"index_uuid"`
		Index        string   `json:"index"`
	}

	IndexProperty struct {
		Type        string `json:"type"`
		Format      string `json:"format"`
		IgnoreAbove int    `json:"ignore_above"`
	}

	IndexMappings struct {
		Properties map[string]*IndexProperty `json:"properties"`
	}

	IndexVersion struct {
		Created string `json:"created"`
	}

	IndexSetting struct {
		CreationDate     string        `json:"creation_date"`
		NumberOfShards   string        `json:"number_of_shards"`
		NumberOfReplicas string        `json:"number_of_replicas"`
		UUID             string        `json:"uuid"`
		Version          *IndexVersion `json:"version"`
		ProvidedName     string        `json:"provided_name"`
	}

	IndexSettings struct {
		Index IndexSetting `json:"index"`
	}

	IndexMeta struct {
		Aliases  interface{}    `json:"aliases"`
		Mappings IndexMappings  `json:"mappings"`
		Settings *IndexSettings `json:"settings"`
	}

	SQLQueryColumn struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	SQLQueryRow []any
)

type ErrorResponse struct {
	Status int    `json:"status"`
	Error  *Error `json:"error"`
}

func (resp ErrorResponse) GetError() error {
	return fmt.Errorf("%s, %s.", resp.Error.Type, resp.Error.Reason)
}

type SQLQueryResponse struct {
	Columns []*SQLQueryColumn `json:"columns"`
	Rows    []SQLQueryRow     `json:"rows"`
}
