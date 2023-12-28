package relalg

import (
	"errors"
	"sync"
)

func ParallelProduct(t1, t2 *Table) (*Table, error) {
	if len(t1.columns) == 0 || len(t2.columns) == 0 {
		return nil, nil
	}
	names := make([]string, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	for i := 0; i < len(t1.columns); i++ {
		names[i] = "l." + t1.columns[i].name
	}
	for i := 0; i < len(t2.columns); i++ {
		names[i+len(t1.columns)] = "r." + t2.columns[i].name
	}
	columns := make([][]interface{}, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	wg := sync.WaitGroup{}
	wg.Add(len(t1.columns) + len(t2.columns))
	for i := 0; i < len(t1.columns); i++ {
		go func(ind int) {
			defer wg.Done()
			for row := 0; row < len(t1.columns[ind].data); row++ {
				for j := 0; j < len(t2.columns[0].data); j++ {
					columns[ind] = append(columns[ind], t1.columns[ind].data[row])
				}
			}
			return
		}(i)
	}
	for i := 0; i < len(t2.columns); i++ {
		go func(ind int) {
			defer wg.Done()
			for j := 0; j < len(t1.columns[ind].data); j++ {
				for row := 0; row < len(t2.columns[ind].data); row++ {
					columns[ind+len(t1.columns)] = append(columns[ind+len(t1.columns)], t2.columns[ind].data[row])
				}
			}
			return
		}(i)
	}
	wg.Wait()
	return trustingCreateTable(names, columns...)
}

func Product(t1, t2 *Table, args ...int) (*Table, error) {
	if len(t1.columns) == 0 || len(t2.columns) == 0 {
		return nil, nil
	}
	for len(args) < 4 {
		args = append(args, 0)
	}
	if args[1] == 0 {
		args[1] = len(t1.columns[0].data)
	}
	if args[3] == 0 {
		args[3] = len(t2.columns[0].data)
	}
	if args[0] >= args[1] || args[2] >= args[3] {
		return nil, errors.New("start index must be less than end index")
	}
	names := make([]string, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	for i := 0; i < len(t1.columns); i++ {
		names[i] = "l." + t1.columns[i].name
	}
	for i := 0; i < len(t2.columns); i++ {
		names[i+len(t1.columns)] = "r." + t2.columns[i].name
	}
	columns1 := make([][]interface{}, len(t1.columns), len(t1.columns))
	for i := 0; i < len(columns1); i++ {
		for row := args[0]; row < args[1]; row++ {
			for j := args[2]; j < args[3]; j++ {
				columns1[i] = append(columns1[i], t1.columns[i].data[row])
			}
		}
	}
	columns2 := make([][]interface{}, len(t2.columns), len(t2.columns))
	for i := 0; i < len(columns2); i++ {
		for j := args[0]; j < args[1]; j++ {
			columns2[i] = append(columns2[i], t2.columns[i].data[args[2]:args[3]]...)
		}
	}
	columns := append(columns1, columns2...)
	return trustingCreateTable(names, columns...)
}
