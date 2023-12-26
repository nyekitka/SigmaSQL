package parsers

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	not = iota
	or
	and
	equals
	less
	greater
	leq
	geq
	neq
)

var CompDesignations = map[string]uint8{
	"=":   equals,
	"!=":  neq,
	">":   greater,
	"<":   less,
	">=":  geq,
	"<=":  leq,
	"or":  or,
	"and": and,
}

type Comparison struct {
	Left, Right string
	Operator    uint8
}

type BooleanTree struct {
	Parent, Left, Right *BooleanTree
	Action              Comparison
}

/*	Operator priority
 *	1 - not
 *	2 - and
 *	3 - or
 */

func StringToBools(s string) (*BooleanTree, error) {
	var results []*BooleanTree
	qns := strings.Split(s, "\"")
	for i := 0; i < len(qns); i += 2 {
		qns[i] = strings.ToLower(qns[i])
	}
	s = strings.Join(qns, "\"")
	openPar := strings.Index(s, "(")
	for openPar != -1 {
		closePar := strings.Index(s[openPar+1:], ")") + openPar + 1
		res, err := StringToBools(s[openPar+1 : closePar])
		if err != nil {
			return nil, err
		}
		results = append(results, res)
		s = s[:openPar] + fmt.Sprintf("{%d}", len(results)-1) + s[closePar+1:]
		openPar = strings.Index(s, "(")
	}

	for compInd := strings.IndexAny(s, "=><!"); compInd != -1; compInd = strings.IndexAny(s, "=><!") {
		// Left operand
		var left_begin, right_end int
		if s[compInd-1] == '"' {
			left_begin = strings.LastIndex(s[:compInd-1], "\"")
		} else if In(rune(s[compInd-1]), "1234567890") {
			left_begin = compInd - 1
			for ; left_begin >= 0 && In(rune(s[left_begin]), "1234567890"); left_begin-- {
			}
			left_begin++
		} else if In(rune(s[compInd-1]), "qwertyuiopasdfghjklzxcvbnm.") {
			left_begin = compInd - 1
			for ; left_begin >= 0 && In(rune(s[left_begin]), "qwertyuiopasdfghjklzxcvbnm."); left_begin-- {
			}
			left_begin++
		} else {
			return nil, errors.New("unresolved symbols are used")
		}
		// Right operand
		var opLen int
		if s[compInd+1] == '=' {
			opLen = 2
		} else {
			opLen = 1
		}
		if s[compInd+opLen] == '"' {
			right_end = strings.Index(s[compInd+opLen+1:], "\"") + compInd + opLen + 1
		} else if In(rune(s[compInd+opLen]), "1234567890") {
			right_end = compInd + opLen
			for ; right_end < len(s) && In(rune(s[right_end]), "1234567890"); right_end++ {
			}
			right_end--
		} else if In(rune(s[compInd+opLen]), "qwertyuiopasdfghjklzxcvbnm.") {
			right_end = compInd + opLen
			for ; right_end < len(s) && In(rune(s[right_end]), "qwertyuiopasdfghjklzxcvbnm."); right_end++ {
			}
			right_end--
		} else {
			return nil, errors.New("unresolved symbols are used")
		}
		comp := Comparison{Operator: CompDesignations[s[compInd:compInd+opLen]],
			Left:  s[left_begin:compInd],
			Right: s[compInd+opLen : right_end+1]}
		res := BooleanTree{Action: comp}
		results = append(results, &res)
		s = s[:left_begin] + fmt.Sprintf("{%d}", len(results)-1) + s[right_end+1:]
	}

	opInd := strings.Index(s, " not ")
	if opInd == -1 {
		opInd = strings.Index(s, " and ")
	}
	if opInd == -1 {
		opInd = strings.Index(s, " or ")
	}
	for opInd != -1 {
		if s[opInd+1] == 'n' {
			langle := opInd + 5
			if langle >= len(s) || s[langle] != '{' {
				return nil, errors.New("not Operator isn't applicable")
			}
			rangle := strings.Index(s[langle+1:], "}")
			if rangle == -1 {
				return nil, errors.New("unresolved symbols are used")
			}
			ref, err := strconv.Atoi(s[langle+1 : rangle])
			if err != nil {
				return nil, errors.New("unresolved symbols are used")
			}
			res := BooleanTree{Left: results[ref], Action: Comparison{Operator: not}}
			results[ref].Parent = &res
			results = append(append(results[:ref], &res), results[ref+1:]...)
			s = s[:opInd] + s[langle:]
		} else {
			// Left operand
			rangle := opInd - 1
			if rangle <= 1 || s[rangle] != '}' {
				switch s[opInd] {
				case 'a':
					return nil, errors.New("and Operator isn't applicable")
				default:
					return nil, errors.New("or Operator isn't applicable")
				}
			}
			langle := strings.LastIndex(s[:rangle], "{")
			if langle == -1 {
				return nil, errors.New("unresolved symbols are used")
			}
			leftBorder := langle
			lref, err := strconv.Atoi(s[langle+1 : rangle])
			if err != nil {
				return nil, errors.New("unresolved symbols are used")
			}

			//Right operand
			if s[opInd+1] == 'a' {
				langle = opInd + 5
			} else {
				langle = opInd + 4
			}
			if langle >= len(s) || s[langle] != '{' {
				switch s[opInd] {
				case 'a':
					return nil, errors.New("and Operator isn't applicable")
				default:
					return nil, errors.New("or Operator isn't applicable")
				}
			}
			rangle = strings.Index(s[langle+1:], "}") + langle + 1
			if rangle == -1 {
				return nil, errors.New("unresolved symbols are used")
			}
			rref, rerr := strconv.Atoi(s[langle+1 : rangle])
			if rerr != nil {
				return nil, errors.New("unresolved symbols are used")
			}
			res := BooleanTree{Left: results[lref], Right: results[rref], Action: Comparison{Operator: uint8(langle - opInd - 3)}}
			results[lref].Parent = &res
			results[rref].Parent = &res
			results = append(results, &res)
			s = s[:leftBorder] + fmt.Sprintf("{%d}", len(results)-1) + s[rangle+1:]
		}
		opInd = strings.Index(s, " not ")
		if opInd == -1 {
			opInd = strings.Index(s, " and ")
		}
		if opInd == -1 {
			opInd = strings.Index(s, " or ")
		}
	}
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		ref, err := strconv.Atoi(s[1 : len(s)-1])
		if err != nil {
			return nil, err
		} else {
			return results[ref], nil
		}
	} else {
		return nil, errors.New("query has some extra symbols")
	}
}
