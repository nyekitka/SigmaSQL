package relalg

import (
	"SigmaSQL/parsers"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

func doesFitRow(table *Table, rowInd int, tree *parsers.BooleanTree) (res bool, resErr error) {
	res = false
	resErr = nil
	if tree.Left == nil {
		var leftVal, rightVal interface{}
		if tree.Action.Left[0] == '"' {
			leftVal = tree.Action.Left[1 : len(tree.Action.Left)-1]
		} else if unicode.IsDigit(rune(tree.Action.Left[0])) {
			leftVal, _ = strconv.Atoi(tree.Action.Left)
		} else {
			i := 0
			for ; i < len(table.columns); i++ {
				if table.columns[i].name == tree.Action.Left {
					break
				}
			}
			if i == len(table.columns) {
				resErr = fmt.Errorf("there is no column named %s", tree.Action.Left)
				return
			} else {
				leftVal = table.columns[i].data[rowInd]
			}
		}
		if tree.Action.Right[0] == '"' {
			rightVal = tree.Action.Right[1 : len(tree.Action.Right)-1]
		} else if unicode.IsDigit(rune(tree.Action.Right[0])) {
			rightVal, resErr = strconv.Atoi(tree.Action.Right)
			if resErr != nil {
				return
			}
		} else {
			i := 0
			for ; i < len(table.columns); i++ {
				if table.columns[i].name == tree.Action.Right {
					break
				}
			}
			if i == len(table.columns) {
				resErr = fmt.Errorf("there is no column named %s", tree.Action.Right)
				return
			} else {
				rightVal = table.columns[i].data[rowInd]
			}
		}
		if tree.Action.Operator == parsers.CompDesignations["="] {
			res = leftVal == rightVal
		} else if tree.Action.Operator == parsers.CompDesignations["!="] {
			res = leftVal != rightVal
		} else {
			_, isLeftInt := leftVal.(int)
			_, isRightInt := rightVal.(int)
			if isLeftInt != isRightInt {
				resErr = errors.New("cannot compare string and int")
			} else if isLeftInt {
				val1, _ := leftVal.(int)
				val2, _ := rightVal.(int)
				switch tree.Action.Operator {
				case parsers.CompDesignations[">"]:
					res = val1 > val2
				case parsers.CompDesignations[">="]:
					res = val1 >= val2
				case parsers.CompDesignations["<="]:
					res = val1 <= val2
				case parsers.CompDesignations["<"]:
					res = val1 < val2
				}
			} else {
				val1, _ := leftVal.(string)
				val2, _ := rightVal.(string)
				switch tree.Action.Operator {
				case parsers.CompDesignations[">"]:
					res = val1 > val2
				case parsers.CompDesignations[">="]:
					res = val1 >= val2
				case parsers.CompDesignations["<="]:
					res = val1 <= val2
				case parsers.CompDesignations["<"]:
					res = val1 < val2
				}
			}
		}
	} else if tree.Right == nil {
		res, resErr = doesFitRow(table, rowInd, tree.Left)
		if resErr == nil {
			res = !res
		}
	} else {
		var leftVal, rightVal bool
		leftVal, resErr = doesFitRow(table, rowInd, tree.Left)
		if resErr != nil {
			return
		}
		rightVal, resErr = doesFitRow(table, rowInd, tree.Right)
		if resErr != nil {
			return
		}
		switch tree.Action.Operator {
		case parsers.CompDesignations["or"]:
			res = leftVal || rightVal
		default:
			res = leftVal && rightVal
		}
	}
	return
}

func checkTree(tree *parsers.BooleanTree, table *Table) error {
	var isLeftInt, isRightInt bool
	if tree.Left != nil {
		err := checkTree(tree.Left, table)
		if err != nil {
			return err
		}
	} else {
		if strings.HasPrefix(tree.Action.Left, "\"") {
			isLeftInt = false
		} else if unicode.IsDigit(rune(tree.Action.Left[0])) {
			isLeftInt = true
		} else {
			isFound := false
			for i := 0; i < len(table.columns); i++ {
				if table.columns[i].name == tree.Action.Left {
					isFound = true
					isLeftInt = table.columns[i].dataType.Name() == "int"
					break
				}
			}
			if !isFound {
				return fmt.Errorf("table has no column named %s", tree.Action.Left)
			}
		}
	}
	if tree.Right != nil {
		err := checkTree(tree.Right, table)
		if err != nil {
			return err
		}
	} else {
		if strings.HasPrefix(tree.Action.Right, "\"") {
			isRightInt = false
		} else if unicode.IsDigit(rune(tree.Action.Right[0])) {
			isRightInt = true
		} else {
			isFound := false
			for i := 0; i < len(table.columns); i++ {
				if table.columns[i].name == tree.Action.Right {
					isFound = true
					isRightInt = table.columns[i].dataType.Name() == "int"
					break
				}
			}
			if !isFound {
				return fmt.Errorf("table has no column named %s", tree.Action.Right)
			}
		}
	}
	if isLeftInt != isRightInt {
		return errors.New("cannot compare string and int")
	} else {
		return nil
	}
}

func ParallelLimitation(table *Table, tree *parsers.BooleanTree) (*Table, error) {
	if len(table.columns) == 0 {
		return table, nil
	}
	names := make([]string, len(table.columns))
	for i := 0; i < len(names); i++ {
		names[i] = table.columns[i].name
	}
	err := checkTree(tree, table)
	if err != nil {
		return nil, err
	}
	columns := make([][]interface{}, len(table.columns), len(table.columns))
	f := func(startInd, endInd int, mutex *sync.Mutex, wg *sync.WaitGroup) {
		defer wg.Done()
		for k := startInd; k < endInd; k++ {
			res, _ := doesFitRow(table, k, tree)
			if res {
				mutex.Lock()
				for i := 0; i < len(columns); i++ {
					columns[i] = append(columns[i], table.columns[i].data[k])
				}
				mutex.Unlock()
			}
		}
		return
	}
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	if len(table.columns[0].data) <= MaxGoroutinesPerProc {
		wg.Add(len(table.columns[0].data))
		for i := 0; i < len(table.columns[0].data); i++ {
			go f(i, i+1, &mutex, &wg)
		}
	} else {
		wg.Add(MaxGoroutinesPerProc)
		for i := 0; i < MaxGoroutinesPerProc; i++ {
			numPerGorout := len(table.columns[0].data) / MaxGoroutinesPerProc
			extraNum := len(table.columns[0].data) % MaxGoroutinesPerProc
			if i < extraNum {
				go f(i*(numPerGorout+1), (i+1)*(numPerGorout+1), &mutex, &wg)
			} else {
				go f((numPerGorout+1)*extraNum+(i-extraNum)*numPerGorout,
					(numPerGorout+1)*extraNum+(i-extraNum+1)*numPerGorout,
					&mutex, &wg)
			}
		}
	}
	wg.Wait()
	return trustingCreateTable(names, columns...)
}

func Limitation(table *Table, tree *parsers.BooleanTree) (*Table, error) {
	if len(table.columns) == 0 {
		return table, nil
	}
	names := make([]string, len(table.columns))
	columns := make([][]interface{}, len(table.columns), len(table.columns))
	for rowInd := 0; rowInd < len(table.columns[0].data); rowInd++ {
		doesFit, err := doesFitRow(table, rowInd, tree)
		if err != nil {
			return nil, err
		} else if doesFit {
			for i := 0; i < len(columns); i++ {
				columns[i] = append(columns[i], table.columns[i].data[rowInd])
				if names[i] == "" {
					names[i] = table.columns[i].name
				}
			}
		}
	}
	return trustingCreateTable(names, columns...)
}
