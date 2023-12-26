package relalg

import (
	"errors"
	"fmt"
	"reflect"
)

type column struct {
	data     []interface{}
	dataType reflect.Type
	name     string
}

type Table struct {
	columns []column
}

func trustingCreateTable(columnsNames []string, columns ...[]interface{}) (result *Table, err error) {
	result = &Table{}
	result.columns = make([]column, len(columns), len(columns))
	for i := 0; i < len(columns); i++ {
		var current = columns[i][0]
		result.columns[i] = column{
			data:     columns[i],
			name:     columnsNames[i],
			dataType: reflect.TypeOf(current)}
	}
	return result, nil
}

func CreateTable(columnsNames []string, columns ...[]interface{}) (result *Table, err error) {
	result = &Table{}
	result.columns = make([]column, len(columns), len(columns))
	for i := 0; i < len(columns); i += 1 {
		var current = columns[i][0]
		if len(columns[i]) != len(columns[0]) {
			return nil, errors.New("columns are not the same size")
		}
		result.columns[i] = column{
			name:     columnsNames[i],
			dataType: reflect.TypeOf(current),
			data:     make([]interface{}, 0, len(columns[0]))}
	}
	uniqueRows := make(map[string]bool)
	for i := 0; i < len(columns[0]); i++ {
		row := ""
		for j := 0; j < len(columns); j++ {
			switch columns[j][i].(type) {
			case int:
				row += fmt.Sprintf("%d", columns[j][i])
			default:
				row += columns[j][i].(string)
			}
			if j != len(columns)-1 {
				row += string(rune(0))
			}
		}
		_, ok := uniqueRows[row]
		if !ok {
			for j := 0; j < len(columns); j++ {
				result.columns[j].data = append(result.columns[j].data, columns[j][i])
			}
			uniqueRows[row] = true
		}
	}
	return result, nil
}
