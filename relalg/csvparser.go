package relalg

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"reflect"
	"strconv"
)

func ReadTableFrom(path string) (*Table, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	names, err := reader.Read()
	if err == io.EOF {
		return nil, errors.New("given file is empty")
	} else if err != nil {
		return nil, err
	}
	columns := make([][]interface{}, len(names), len(names))
	types := make([]reflect.Type, len(names), len(names))
	var row []string
	firstRow := true
	for {
		row, err = reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if len(row) != len(columns) {
			return nil, errors.New("number of values in tuple doesn't equal to number of columns")
		}
		for i, val := range row {
			if firstRow {
				valInt, ok := strconv.Atoi(val)
				if ok != nil {
					types[i] = reflect.TypeOf("")
					columns[i] = append(columns[i], val)
				} else {
					types[i] = reflect.TypeOf(0)
					columns[i] = append(columns[i], valInt)
				}
			} else {
				if types[i].Name() == "string" {
					columns[i] = append(columns[i], val)
				} else {
					valInt, ok := strconv.Atoi(val)
					if ok != nil {
						return nil, errors.New("string value in a column that contains integers")
					} else {
						columns[i] = append(columns[i], valInt)
					}
				}
			}
		}
		firstRow = false
	}
	if len(columns[0]) == 0 {
		return CreateEmptyTable(names, types)
	} else {
		return CreateTable(names, columns...)
	}
}
