package relalg

func Union(t1, t2 *Table) (*Table, error) {
	addTable, err := Subtraction(t1, t2)
	if err != nil {
		return addTable, err
	}
	for i := 0; i < len(addTable.columns); i++ {
		addTable.columns[i].data = append(addTable.columns[i].data, t2.columns[i].data...)
	}
	return addTable, err
}
