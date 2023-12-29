package relalg

import (
	"fmt"
)

func Projection(table *Table, fields []string) (*Table, error) {
	if table == nil || len(table.Columns) == 0 || len(fields) == 0 {
		return nil, nil
	}
	columns := make([][]interface{}, 0, len(table.Columns))
	for _, field := range fields {
		isFound := false
		for _, col := range table.Columns {
			if col.Name == field {
				columns = append(columns, col.Data)
				isFound = true
				break
			}
		}
		if !isFound {
			return nil, fmt.Errorf("there's no field named \"%s\"", field)
		}
	}
	return CreateTable(fields, columns...)
}
