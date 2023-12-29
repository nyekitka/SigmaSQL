package relalg

func ParallelUnion(t1, t2 *Table, MaxGoroutines int) (*Table, error) {
	addTable, err := ParallelSubtraction(t1, t2, MaxGoroutines)
	if err != nil {
		return addTable, err
	}
	for i := 0; i < len(addTable.Columns); i++ {
		addTable.Columns[i].Data = append(addTable.Columns[i].Data, t2.Columns[i].Data...)
	}
	return addTable, err
}

func Union(t1, t2 *Table) (*Table, error) {
	addTable, err := Subtraction(t1, t2)
	if err != nil {
		return addTable, err
	}
	for i := 0; i < len(addTable.Columns); i++ {
		addTable.Columns[i].Data = append(addTable.Columns[i].Data, t2.Columns[i].Data...)
	}
	return addTable, err
}
