package main

import (
	"SigmaSQL/relalg"
	"fmt"
)

func main() {
	arr1 := make([]interface{}, 5, 5)
	for i := 0; i < 5; i++ {
		arr1[i] = i
	}
	arr2 := make([]interface{}, 5, 5)
	arr2[0] = "Nikita"
	arr2[1] = "Masha"
	arr2[2] = "Artem"
	arr2[3] = "Dasha"
	arr2[4] = "Andrey"
	arr3 := make([]interface{}, 5, 5)
	arr3[0] = 100000
	arr3[1] = 20000
	arr3[2] = 0
	arr3[3] = 20000
	arr3[4] = 20000
	arr4 := make([]interface{}, 5, 5)
	arr4[0] = "male"
	arr4[1] = "female"
	arr4[2] = "male"
	arr4[3] = "female"
	arr4[4] = "male"
	t1, _ := relalg.CreateTable([]string{"id", "name", "salary"}, arr1, arr2, arr3)
	t2, _ := relalg.Projection(t1, []string{"salary"})
	fmt.Print(t2)
	//t2, _ := relalg.CreateTable([]string{"id", "gender"}, arr1, arr4)
	//tree, _ := parsers.StringToBools("L.id=R.id and R.gender=\"female\"")
	//t3, err := relalg.Join(t1, t2, tree)
	//fmt.Print(t3, err)
}
