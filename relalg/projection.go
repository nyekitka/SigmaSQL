package relalg

import (
	"fmt"
)

func Projection(table *Table, fields []string) (*Table, error) {
	columns := make([][]interface{}, 0, len(table.columns))
	for _, field := range fields {
		isFound := false
		for _, col := range table.columns {
			if col.name == field {
				columns = append(columns, col.data)
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
