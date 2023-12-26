package relalg

func Product(t1, t2 *Table) (*Table, error) {
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
	columns1 := make([][]interface{}, len(t1.columns), len(t1.columns))
	for i := 0; i < len(columns1); i++ {
		for row := 0; row < len(t1.columns[i].data); row++ {
			for j := 0; j < len(t2.columns[0].data); j++ {
				columns1[i] = append(columns1[i], t1.columns[i].data[row])
			}
		}
	}
	columns2 := make([][]interface{}, len(t2.columns), len(t2.columns))
	for i := 0; i < len(columns2); i++ {
		for j := 0; j < len(t1.columns[0].data); j++ {
			columns2[i] = append(columns2[i], t2.columns[i].data...)
		}
	}
	columns := append(columns1, columns2...)
	return trustingCreateTable(names, columns...)
}
