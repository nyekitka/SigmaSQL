package relalg

import (
	"errors"
	"fmt"
	"reflect"
)

type column struct {
	Data     []interface{}
	DataType reflect.Type
	Name     string
}

type Table struct {
	Columns []column
}

func trustingCreateTable(columnsNames []string, columns ...[]interface{}) (result *Table, err error) {
	result = &Table{}
	result.Columns = make([]column, len(columns), len(columns))
	for i := 0; i < len(columns); i++ {
		if len(columns[i]) == 0 {
			return nil, errors.New("empty columns are given")
		}
		var current = columns[i][0]
		result.Columns[i] = column{
			Data:     columns[i],
			Name:     columnsNames[i],
			DataType: reflect.TypeOf(current)}
	}
	return result, nil
}

func CreateTable(columnsNames []string, columns ...[]interface{}) (result *Table, err error) {
	result = &Table{}
	result.Columns = make([]column, len(columns), len(columns))
	for i := 0; i < len(columns); i += 1 {
		if len(columns[i]) == 0 {
			return nil, errors.New("empty columns are given")
		}
		var current = columns[i][0]
		if len(columns[i]) != len(columns[0]) {
			return nil, errors.New("columns are not the same size")
		}
		result.Columns[i] = column{
			Name:     columnsNames[i],
			DataType: reflect.TypeOf(current),
			Data:     make([]interface{}, 0, len(columns[0]))}
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
				result.Columns[j].Data = append(result.Columns[j].Data, columns[j][i])
			}
			uniqueRows[row] = true
		}
	}
	return result, nil
}

func CreateEmptyTable(names []string, types []reflect.Type) (*Table, error) {
	if len(names) != len(types) {
		return nil, errors.New("length of names isn't equal to length of types")
	}
	result := &Table{}
	result.Columns = make([]column, len(names), len(names))
	for i := 0; i < len(names); i++ {
		result.Columns[i] = column{
			Data:     make([]interface{}, 0, 10),
			DataType: types[i],
			Name:     names[i],
		}
	}
	return result, nil
}
