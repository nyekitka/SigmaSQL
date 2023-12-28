package main

import (
	"SigmaSQL/parsers"
	"SigmaSQL/relalg"
	"fmt"
)

func main() {
	//runtime.GOMAXPROCS(8)
	//fmt.Println(runtime.NumCPU())
	//n := 10000
	//k := 4
	////names := make([]string, k, k)
	//arrs := make([][]interface{}, k, k)
	//alphabet := "qwertyuiopasdfghjklzxcvbnm"
	//for i := 0; i < n; i++ {
	//	lens := make([]int, k, k)
	//	strs := make([]string, k, k)
	//	for j := 0; j < k; j++ {
	//		lens[j] = rand.Intn(30) + 1
	//	}
	//	for t := 0; t < k; t++ {
	//		for j := 0; j < lens[t]; j++ {
	//			strs[t] += string(alphabet[rand.Intn(26)])
	//		}
	//		arrs[t] = append(arrs[t], strs[t])
	//	}
	//}
	////for i := 0; i < k; i++ {
	////	names[i] = fmt.Sprintf("field%d", i+1)
	////}
	//t1, _ := relalg.CreateTable([]string{"name", "surname"}, arrs[:2]...)
	//t2, _ := relalg.CreateTable([]string{"name", "surname"}, arrs[2:]...)
	//var err error
	//tree, _ := parsers.StringToBools("l.name=r.name")
	//t := time.Now()
	//_, err = relalg.Join1(t1, t2, tree)
	//fmt.Printf("Not parallel (not optimized) - %d\n", time.Since(t).Microseconds())
	//t = time.Now()
	//_, err = relalg.Join2(t1, t2, tree)
	//fmt.Printf("Not parallel (optimized) - %d\n", time.Since(t).Microseconds())
	//t = time.Now()
	//_, err = relalg.ParallelJoin1(t1, t2, tree)
	//fmt.Printf("Parallel (not optimized) - %d\n", time.Since(t).Microseconds())
	//t = time.Now()
	//_, err = relalg.ParallelJoin2(t1, t2, tree)
	//fmt.Printf("Parallel (optimized) - %d\n", time.Since(t).Microseconds())
	//if err != nil {
	//	fmt.Print(err)
	//}
	//fmt.Print(t2)
	//t2, _ := relalg.CreateTable([]string{"id", "gender"}, arr1, arr4)
	//tree, _ := parsers.StringToBools("L.id=R.id and R.gender=\"female\"")
	//t3, err := relalg.Join(t1, t2, tree)
	//fmt.Print(t3, err)
	users, _ := relalg.ReadTableFrom("C:\\Users\\Никита\\Desktop\\users.csv")
	subscriptions, _ := relalg.ReadTableFrom("C:\\Users\\Никита\\Desktop\\subscriptions.csv")
	//mealplans, _ := relalg.ReadTableFrom("C:\\Users\\Никита\\Desktop\\mealplans.csv")
	tree, _ := parsers.StringToBools("l.id=r.userid and r.startday>\"2023-06-01\"")
	t1, _ := relalg.ParallelJoin2(users, subscriptions, tree)
	fmt.Print(t1)
}
