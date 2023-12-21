package main

import (
	"fmt"
	"parsers"
)

func main() {
	res, err := parsers.StringToBools("A.b=\"HeLLo\" and (T.s>=R.s or R.s<5)")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Print(res)
	}
}
