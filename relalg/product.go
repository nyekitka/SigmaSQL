package relalg

import (
	"errors"
	"reflect"
	"sync"
)

func ParallelProduct(t1, t2 *Table, MaxGoroutines int) (*Table, error) {
	if len(t1.Columns) == 0 || len(t2.Columns) == 0 {
		return nil, nil
	}
	names := make([]string, len(t1.Columns)+len(t2.Columns), len(t1.Columns)+len(t2.Columns))
	types := make([]reflect.Type, len(t1.Columns)+len(t2.Columns), len(t1.Columns)+len(t2.Columns))
	for i := 0; i < len(t1.Columns); i++ {
		names[i] = "l." + t1.Columns[i].Name
		types[i] = t1.Columns[i].DataType
	}
	for i := 0; i < len(t2.Columns); i++ {
		names[i+len(t1.Columns)] = "r." + t2.Columns[i].Name
		types[i+len(t1.Columns)] = t2.Columns[i].DataType
	}
	if len(t1.Columns[0].Data) == 0 || len(t2.Columns[0].Data) == 0 {
		return CreateEmptyTable(names, types)
	}
	columns := make([][]interface{}, len(t1.Columns)+len(t2.Columns), len(t1.Columns)+len(t2.Columns))
	wg := sync.WaitGroup{}
	f := func(startInd, endInd int) {
		defer wg.Done()
		for ind := startInd; ind < endInd; ind++ {
			if ind < len(t1.Columns) {
				for row := 0; row < len(t1.Columns[0].Data); row++ {
					for j := 0; j < len(t2.Columns[0].Data); j++ {
						columns[ind] = append(columns[ind], t1.Columns[ind].Data[row])
					}
				}
			} else {
				for j := 0; j < len(t1.Columns[0].Data); j++ {
					for row := 0; row < len(t2.Columns[0].Data); row++ {
						columns[ind] = append(columns[ind], t2.Columns[ind-len(t1.Columns)].Data[row])
					}
				}
			}
		}
		return
	}
	if len(t1.Columns)+len(t2.Columns) <= MaxGoroutines {
		wg.Add(len(t1.Columns) + len(t2.Columns))
		for i := 0; i < len(t1.Columns)+len(t2.Columns); i++ {
			go f(i, i+1)
		}
	} else {
		wg.Add(MaxGoroutines)
		numTasks := (len(t1.Columns) + len(t2.Columns)) / MaxGoroutines
		resTasks := (len(t1.Columns) + len(t2.Columns)) % MaxGoroutines
		usedTasks := 0
		for i := 0; i < numTasks*MaxGoroutines; i += numTasks {
			newUsedTasks := usedTasks
			if newUsedTasks < resTasks {
				newUsedTasks++
			}
			go f(i+usedTasks, i+numTasks+newUsedTasks)
			usedTasks = newUsedTasks
		}
	}
	wg.Wait()
	return trustingCreateTable(names, columns...)
}

func Product(t1, t2 *Table, args ...int) (*Table, error) {
	if len(t1.Columns) == 0 || len(t2.Columns) == 0 {
		return nil, nil
	}
	for len(args) < 4 {
		args = append(args, 0)
	}
	if args[1] == 0 {
		args[1] = len(t1.Columns[0].Data)
	}
	if args[3] == 0 {
		args[3] = len(t2.Columns[0].Data)
	}
	if args[0] >= args[1] || args[2] >= args[3] {
		return nil, errors.New("start index must be less than end index")
	}
	names := make([]string, len(t1.Columns)+len(t2.Columns), len(t1.Columns)+len(t2.Columns))
	types := make([]reflect.Type, len(t1.Columns)+len(t2.Columns), len(t1.Columns)+len(t2.Columns))
	for i := 0; i < len(t1.Columns); i++ {
		names[i] = "l." + t1.Columns[i].Name
		types[i] = t1.Columns[i].DataType
	}
	for i := 0; i < len(t2.Columns); i++ {
		names[i+len(t1.Columns)] = "r." + t2.Columns[i].Name
		types[i+len(t1.Columns)] = t2.Columns[i].DataType
	}
	if len(t1.Columns[0].Data) == 0 || len(t2.Columns[0].Data) == 0 {
		return CreateEmptyTable(names, types)
	}
	columns1 := make([][]interface{}, len(t1.Columns), len(t1.Columns))
	for i := 0; i < len(columns1); i++ {
		for row := args[0]; row < args[1]; row++ {
			for j := args[2]; j < args[3]; j++ {
				columns1[i] = append(columns1[i], t1.Columns[i].Data[row])
			}
		}
	}
	columns2 := make([][]interface{}, len(t2.Columns), len(t2.Columns))
	for i := 0; i < len(columns2); i++ {
		for j := args[0]; j < args[1]; j++ {
			columns2[i] = append(columns2[i], t2.Columns[i].Data[args[2]:args[3]]...)
		}
	}
	columns := append(columns1, columns2...)
	return trustingCreateTable(names, columns...)
}
