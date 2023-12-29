package relalg

import (
	"errors"
	"reflect"
	"sync"
)

func ParallelIntersection(t1, t2 *Table, MaxGoroutines int) (*Table, error) {

	names := make([]string, len(t1.Columns))
	types := make([]reflect.Type, len(t1.Columns))

	// checking if columns of the tables are same

	if len(t1.Columns) != len(t2.Columns) {
		return nil, errors.New("columns of the tables aren't same")
	} else {
		for i := 0; i < len(t1.Columns); i++ {
			if t1.Columns[i].Name != t2.Columns[i].Name || t1.Columns[i].DataType != t2.Columns[i].DataType {
				return nil, errors.New("columns of the tables aren't same")
			}
			names[i] = t1.Columns[i].Name
			types[i] = t1.Columns[i].DataType
		}
	}

	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	columns := make([][]interface{}, len(t1.Columns), len(t1.Columns))

	if len(t1.Columns[0].Data) <= MaxGoroutines {
		wg.Add(len(t1.Columns[0].Data))
		for i := 0; i < len(t1.Columns[0].Data); i++ {
			go intersection(t1, t2, &columns, &mutex, &wg, i, i+1)
		}
	} else {
		wg.Add(MaxGoroutines)
		mainCount := len(t1.Columns[0].Data) / MaxGoroutines
		additCount := len(t1.Columns[0].Data) % MaxGoroutines
		for i := 0; i < additCount; i++ {
			go intersection(t1, t2, &columns, &mutex, &wg, i*(mainCount+1), (i+1)*(mainCount+1))
		}
		for i := 0; i < MaxGoroutines-additCount; i++ {
			go intersection(t1, t2, &columns, &mutex, &wg, i*mainCount+additCount*(mainCount+1), (i+1)*mainCount+additCount*(mainCount+1))
		}
	}
	wg.Wait()
	if columns[0] == nil {
		return CreateEmptyTable(names, types)
	} else {
		return trustingCreateTable(names, columns...)
	}
}

func intersection(t1, t2 *Table, whereToSave *[][]interface{}, mutex *sync.Mutex, wg *sync.WaitGroup, startInd, endInd int) {

	defer wg.Done()

	// looking through the rows of the first table and finding out whether the second table has the same row

	for row1 := startInd; row1 < endInd; row1++ {
		anyEqual := false
		for row2 := 0; row2 < len(t2.Columns[0].Data); row2++ {
			isEqual := true
			for col := 0; col < len(t1.Columns); col++ {
				if t1.Columns[col].Data[row1] != t2.Columns[col].Data[row2] {
					isEqual = false
					break
				}
			}
			if isEqual {
				anyEqual = true
				break
			}
		}
		if anyEqual {
			mutex.Lock()
			for col := 0; col < len(t1.Columns); col++ {
				(*whereToSave)[col] = append((*whereToSave)[col], t1.Columns[col].Data[row1])
			}
			mutex.Unlock()
		}
	}
	return
}

func Intersection(t1 *Table, t2 *Table) (*Table, error) {
	names := make([]string, len(t1.Columns))
	types := make([]reflect.Type, len(t1.Columns))
	// checking if columns of the tables are same

	if len(t1.Columns) != len(t2.Columns) {
		return nil, errors.New("columns of the tables aren't same")
	} else {
		for i := 0; i < len(t1.Columns); i++ {
			if t1.Columns[i].Name != t2.Columns[i].Name || t1.Columns[i].DataType != t2.Columns[i].DataType {
				return nil, errors.New("columns of the tables aren't same")
			}
			names[i] = t1.Columns[i].Name
			types[i] = t1.Columns[i].DataType
		}
	}

	// looking through the rows of the first table and finding out whether the second table has the same row

	columns := make([][]interface{}, len(t1.Columns), len(t1.Columns))

	for row1 := 0; row1 < len(t1.Columns[0].Data); row1++ {
		anyEqual := false
		for row2 := 0; row2 < len(t2.Columns[0].Data); row2++ {
			isEqual := true
			for col := 0; col < len(columns); col++ {
				if t1.Columns[col].Data[row1] != t2.Columns[col].Data[row2] {
					isEqual = false
					break
				}
			}
			if isEqual {
				anyEqual = true
				break
			}
		}
		if anyEqual {
			for col := 0; col < len(columns); col++ {
				columns[col] = append(columns[col], t1.Columns[col].Data[row1])
			}
		}
	}
	if columns[0] == nil {
		return CreateEmptyTable(names, types)
	} else {
		return trustingCreateTable(names, columns...)
	}
}
