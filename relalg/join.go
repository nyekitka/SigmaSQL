package relalg

import "SigmaSQL/parsers"

func Join(t1, t2 *Table, tree *parsers.BooleanTree) (*Table, error) {
	crossTable, err := Product(t1, t2)
	if err != nil {
		return nil, err
	} else {
		var res *Table
		res, err = Limitation(crossTable, tree)
		return res, err
	}
}
