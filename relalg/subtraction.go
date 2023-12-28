package relalg

import (
	"errors"
	"sync"
)

var MaxGoroutinesPerProc = 10000

func ParallelSubtraction(t1, t2 *Table) (*Table, error) {

	names := make([]string, len(t1.columns))

	// checking if columns of the tables are same

	if len(t1.columns) != len(t2.columns) {
		return nil, errors.New("columns of the tables aren't same")
	} else {
		for i := 0; i < len(t1.columns); i++ {
			if t1.columns[i].name != t2.columns[i].name || t1.columns[i].dataType != t2.columns[i].dataType {
				return nil, errors.New("columns of the tables aren't same")
			}
			names[i] = t1.columns[i].name
		}
	}

	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	columns := make([][]interface{}, len(t1.columns), len(t1.columns))

	if len(t1.columns[0].data) <= MaxGoroutinesPerProc {
		wg.Add(len(t1.columns[0].data))
		for i := 0; i < len(t1.columns[0].data); i++ {
			go subtraction(t1, t2, &columns, &mutex, &wg, i, i+1)
		}
	} else {
		wg.Add(MaxGoroutinesPerProc)
		mainCount := len(t1.columns[0].data) / MaxGoroutinesPerProc
		additCount := len(t1.columns[0].data) % MaxGoroutinesPerProc
		for i := 0; i < additCount; i++ {
			go subtraction(t1, t2, &columns, &mutex, &wg, i*(mainCount+1), (i+1)*(mainCount+1))
		}
		for i := 0; i < MaxGoroutinesPerProc-additCount; i++ {
			go subtraction(t1, t2, &columns, &mutex, &wg, i*mainCount+additCount*(mainCount+1), (i+1)*mainCount+additCount*(mainCount+1))
		}
	}
	wg.Wait()
	return trustingCreateTable(names, columns...)
}

func subtraction(t1, t2 *Table, whereToSave *[][]interface{}, mutex *sync.Mutex, wg *sync.WaitGroup, startInd, endInd int) {

	defer wg.Done()

	// looking through the rows of the first table and finding out whether the second table has the same row

	for row1 := startInd; row1 < endInd; row1++ {
		anyEqual := false
		for row2 := 0; row2 < len(t2.columns[0].data); row2++ {
			isEqual := true
			for col := 0; col < len(t1.columns); col++ {
				if t1.columns[col].data[row1] != t2.columns[col].data[row2] {
					isEqual = false
					break
				}
			}
			if isEqual {
				anyEqual = true
				break
			}
		}
		if !anyEqual {
			mutex.Lock()
			for col := 0; col < len(t1.columns); col++ {
				(*whereToSave)[col] = append((*whereToSave)[col], t1.columns[col].data[row1])
			}
			mutex.Unlock()
		}
	}
	return
}

func Subtraction(t1 *Table, t2 *Table) (*Table, error) {
	names := make([]string, len(t1.columns))

	// checking if columns of the tables are same

	if len(t1.columns) != len(t2.columns) {
		return nil, errors.New("columns of the tables aren't same")
	} else {
		for i := 0; i < len(t1.columns); i++ {
			if t1.columns[i].name != t2.columns[i].name || t1.columns[i].dataType != t2.columns[i].dataType {
				return nil, errors.New("columns of the tables aren't same")
			}
			names[i] = t1.columns[i].name
		}
	}

	// looking through the rows of the first table and finding out whether the second table has the same row

	columns := make([][]interface{}, len(t1.columns), len(t1.columns))

	for row1 := 0; row1 < len(t1.columns[0].data); row1++ {
		anyEqual := false
		for row2 := 0; row2 < len(t2.columns[0].data); row2++ {
			isEqual := true
			for col := 0; col < len(columns); col++ {
				if t1.columns[col].data[row1] != t2.columns[col].data[row2] {
					isEqual = false
					break
				}
			}
			if isEqual {
				anyEqual = true
				break
			}
		}
		if !anyEqual {
			for col := 0; col < len(columns); col++ {
				columns[col] = append(columns[col], t1.columns[col].data[row1])
			}
		}
	}
	res, err := trustingCreateTable(names, columns...)
	return res, err
}
