package relalg

import (
	"SigmaSQL/parsers"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

func doesFitCombRow(t1, t2 *Table, tree *parsers.BooleanTree, ind1, ind2 int) bool {
	res := false
	if tree.Left == nil {
		var leftVal, rightVal interface{}
		if tree.Action.Left[0] == '"' {
			leftVal = tree.Action.Left[1 : len(tree.Action.Left)-1]
		} else if unicode.IsDigit(rune(tree.Action.Left[0])) {
			leftVal, _ = strconv.Atoi(tree.Action.Left)
		} else {
			var table *Table
			var ind int
			if strings.HasPrefix(tree.Action.Left, "l.") {
				table = t1
				ind = ind1
			} else {
				table = t2
				ind = ind2
			}
			i := 0
			for ; i < len(table.columns); i++ {
				if table.columns[i].name == tree.Action.Left[2:] {
					break
				}
			}
			leftVal = table.columns[i].data[ind]
		}
		if tree.Action.Right[0] == '"' {
			rightVal = tree.Action.Right[1 : len(tree.Action.Right)-1]
		} else if unicode.IsDigit(rune(tree.Action.Right[0])) {
			rightVal, _ = strconv.Atoi(tree.Action.Right)
		} else {
			var table *Table
			var ind int
			if strings.HasPrefix(tree.Action.Right, "l.") {
				table = t1
				ind = ind1
			} else {
				table = t2
				ind = ind2
			}
			i := 0
			for ; i < len(table.columns); i++ {
				if table.columns[i].name == tree.Action.Right[2:] {
					break
				}
			}
			rightVal = table.columns[i].data[ind]
		}
		if tree.Action.Operator == parsers.CompDesignations["="] {
			res = leftVal == rightVal
		} else if tree.Action.Operator == parsers.CompDesignations["!="] {
			res = leftVal != rightVal
		} else {
			_, isLeftInt := leftVal.(int)
			if isLeftInt {
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
		res = !doesFitCombRow(t1, t2, tree.Left, ind1, ind2)
	} else {
		var leftVal, rightVal bool
		leftVal = doesFitCombRow(t1, t2, tree.Left, ind1, ind2)
		rightVal = doesFitCombRow(t1, t2, tree.Right, ind1, ind2)
		switch tree.Action.Operator {
		case parsers.CompDesignations["or"]:
			res = leftVal || rightVal
		default:
			res = leftVal && rightVal
		}
	}
	return res
}

func checkCombTree(tree *parsers.BooleanTree, t1, t2 *Table) error {
	var isLeftInt, isRightInt bool
	if tree.Left != nil {
		err := checkCombTree(tree.Left, t1, t2)
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
			var table *Table
			if strings.HasPrefix(tree.Action.Left, "l.") {
				table = t1
			} else if strings.HasPrefix(tree.Action.Left, "r.") {
				table = t2
			} else {
				return fmt.Errorf("field named %s has no connection to any table", tree.Action.Left)
			}
			for i := 0; i < len(table.columns); i++ {
				if table.columns[i].name == tree.Action.Left[2:] {
					isFound = true
					isLeftInt = table.columns[i].dataType.Name() == "int"
					break
				}
			}
			if !isFound {
				return fmt.Errorf("table has no column named %s", tree.Action.Left[2:])
			}
		}
	}
	if tree.Right != nil {
		err := checkCombTree(tree.Right, t1, t2)
		if err != nil {
			return err
		}
	} else {
		if strings.HasPrefix(tree.Action.Right, "\"") {
			isRightInt = false
		} else if unicode.IsDigit(rune(tree.Action.Right[0])) {
			isRightInt = true
		} else {
			var table *Table
			if strings.HasPrefix(tree.Action.Right, "l.") {
				table = t1
			} else if strings.HasPrefix(tree.Action.Right, "r.") {
				table = t2
			} else {
				return fmt.Errorf("field named %s has no connection to any table", tree.Action.Right)
			}
			isFound := false
			for i := 0; i < len(table.columns); i++ {
				if table.columns[i].name == tree.Action.Right[2:] {
					isFound = true
					isRightInt = table.columns[i].dataType.Name() == "int"
					break
				}
			}
			if !isFound {
				return fmt.Errorf("table has no column named %s", tree.Action.Right[2:])
			}
		}
	}
	if isLeftInt != isRightInt {
		return errors.New("cannot compare string and int")
	} else {
		return nil
	}
}

// 1 - not optimized
// 2 - optimized

func Join2(t1, t2 *Table, tree *parsers.BooleanTree) (*Table, error) {
	if len(t1.columns) == 0 || len(t2.columns) == 0 {
		return nil, nil
	}
	err := checkCombTree(tree, t1, t2)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	for i := 0; i < len(t1.columns); i++ {
		names[i] = "l." + t1.columns[i].name
	}
	for i := 0; i < len(t2.columns); i++ {
		names[i+len(t1.columns)] = "r." + t2.columns[i].name
	}
	types := make([]reflect.Type, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	for i := 0; i < len(t1.columns)+len(t2.columns); i++ {
		if i < len(t1.columns) {
			types[i] = t1.columns[i].dataType
		} else {
			types[i] = t2.columns[i-len(t1.columns)].dataType
		}
	}
	columns := make([][]interface{}, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	for row1 := 0; row1 < len(t1.columns[0].data); row1++ {
		for row2 := 0; row2 < len(t2.columns[0].data); row2++ {
			if doesFitCombRow(t1, t2, tree, row1, row2) {
				for i := 0; i < len(t1.columns)+len(t2.columns); i++ {
					if i < len(t1.columns) {
						columns[i] = append(columns[i], t1.columns[i].data[row1])
					} else {
						columns[i] = append(columns[i], t2.columns[i-len(t1.columns)].data[row2])
					}
				}
			}
		}
	}
	if len(columns) != 0 {
		return trustingCreateTable(names, columns...)
	} else {
		return CreateEmptyTable(names, types)
	}
}

func Join1(t1, t2 *Table, tree *parsers.BooleanTree) (*Table, error) {
	crossTable, err := Product(t1, t2)
	if err != nil {
		return nil, err
	} else {
		var res *Table
		res, err = Limitation(crossTable, tree)
		return res, err
	}
}

func ParallelJoin1(t1, t2 *Table, tree *parsers.BooleanTree) (*Table, error) {
	crossTable, err := ParallelProduct(t1, t2)
	if err != nil {
		return nil, err
	} else {
		var res *Table
		res, err = ParallelLimitation(crossTable, tree)
		return res, err
	}
}

func ParallelJoin2(t1, t2 *Table, tree *parsers.BooleanTree) (*Table, error) {
	if len(t1.columns) == 0 || len(t2.columns) == 0 {
		return nil, nil
	}
	err := checkCombTree(tree, t1, t2)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	for i := 0; i < len(t1.columns); i++ {
		names[i] = "l." + t1.columns[i].name
	}
	for i := 0; i < len(t2.columns); i++ {
		names[i+len(t1.columns)] = "r." + t2.columns[i].name
	}
	types := make([]reflect.Type, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	for i := 0; i < len(t1.columns)+len(t2.columns); i++ {
		if i < len(t1.columns) {
			types[i] = t1.columns[i].dataType
		} else {
			types[i] = t2.columns[i-len(t1.columns)].dataType
		}
	}
	columns := make([][]interface{}, len(t1.columns)+len(t2.columns), len(t1.columns)+len(t2.columns))
	f := func(startInd1, startInd2, endInd1, endInd2 int, wg *sync.WaitGroup, mutex *sync.Mutex) {
		defer wg.Done()
		for row1 := startInd1; row1 < endInd1; row1++ {
			for row2 := startInd2; row2 < endInd2; row2++ {
				if doesFitCombRow(t1, t2, tree, row1, row2) {
					mutex.Lock()
					for i := 0; i < len(t1.columns)+len(t2.columns); i++ {
						if i < len(t1.columns) {
							columns[i] = append(columns[i], t1.columns[i].data[row1])
						} else {
							columns[i] = append(columns[i], t2.columns[i-len(t1.columns)].data[row2])
						}
					}
					mutex.Unlock()
				}
			}
		}
		return
	}
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	if len(t1.columns[0].data)*len(t2.columns[0].data) <= MaxGoroutinesPerProc {
		wg.Add(len(t1.columns[0].data) * len(t2.columns[0].data))
		for i := 0; i < len(t1.columns[0].data); i++ {
			for j := 0; j < len(t2.columns[0].data); j++ {
				go f(i, j, i+1, j+1, &wg, &mutex)
			}
		}
	} else {
		wg.Add(MaxGoroutinesPerProc)
		nSplit := int(math.Sqrt(float64(MaxGoroutinesPerProc)))
		if len(t1.columns[0].data) < nSplit {
			nSplit = MaxGoroutinesPerProc / len(t1.columns[0].data)
			resExtr := len(t2.columns[0].data) % nSplit
			incr := len(t2.columns[0].data) / nSplit
			for i := 0; i < len(t1.columns[0].data); i++ {
				add := 0
				for j := 0; j < nSplit*incr; j += incr {
					newadd := add
					if newadd < resExtr {
						newadd++
					}
					go f(i, j+add, i+1, j+incr+newadd, &wg, &mutex)
					add = newadd
				}
			}
		} else if len(t2.columns[0].data) < nSplit {
			nSplit = MaxGoroutinesPerProc / len(t2.columns[0].data)
			resExtr := len(t1.columns[0].data) % nSplit
			incr := len(t1.columns[0].data) / nSplit
			for i := 0; i < len(t2.columns[0].data); i++ {
				add := 0
				for j := 0; j < nSplit*incr; j += incr {
					newadd := add
					if newadd < resExtr {
						newadd++
					}
					go f(i, j+add, i+1, j+incr+newadd, &wg, &mutex)
					add = newadd
				}
			}
		} else {
			incr1 := len(t1.columns[0].data) / nSplit
			incr2 := len(t2.columns[0].data) / nSplit
			resExtr1 := len(t1.columns[0].data) % nSplit
			resExtr2 := len(t2.columns[0].data) % nSplit
			add1 := 0
			for i := 0; i < nSplit*incr1; i += incr1 {
				newadd1 := add1
				if newadd1 < resExtr1 {
					newadd1++
				}
				add2 := 0
				for j := 0; j < nSplit*incr2; j += incr2 {
					newadd2 := add2
					if newadd2 < resExtr2 {
						newadd2++
					}
					go f(i+add1, j+add2, i+incr1+newadd1, j+incr2+newadd2, &wg, &mutex)
					add2 = newadd2
				}
				add1 = newadd1
			}
		}
	}
	wg.Wait()
	if len(columns) != 0 {
		return trustingCreateTable(names, columns...)
	} else {
		return CreateEmptyTable(names, types)
	}
}
