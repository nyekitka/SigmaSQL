package parsers

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

const (
	empty = iota
	union
	intersect
	subtraction
	product
	join
	division
	projection
	limit
)

var Designations = map[uint8]uint8{
	'+':  union,
	'&':  intersect,
	'\\': subtraction,
	'*':  product,
}

type action struct {
	_type  uint8
	_name  string
	_attrs []string
}

type SyntaxTree struct {
	Parent, Left, Right *SyntaxTree
	Data                action
}

func In(c rune, _range string) bool {
	for _, _c := range _range {
		if c == rune(_c) {
			return true
		}
	}
	return false
}

func StringToTree(s string, results ...*SyntaxTree) (*SyntaxTree, error) {
	//"results" are independent expressions that marked in s as "<n>"

	// we remove the brackets from the expression by recursively building expression trees inside the brackets

	openPar := strings.LastIndex(s, "(")
	for openPar != -1 {
		closePar := strings.Index(s[openPar+1:], ")") + openPar + 1
		res, err := StringToTree(s[openPar+1:closePar], results...)
		if err != nil {
			return res, err
		} else {
			results = append(results, res)
			s = s[:openPar] + fmt.Sprintf("<%d>", len(results)-1) + s[closePar+1:]
		}
		openPar = strings.LastIndex(s, "(")
	}

	// finding limitations by finding substrings like [...,...,...] and
	// projections by finding substrings which is represents unary operation

	openPar = strings.Index(s, "[")
	for openPar != -1 {
		closePar := strings.Index(s[openPar+1:], "]") + openPar + 1
		var _type uint8
		if strings.Contains(s[openPar+1:closePar], ",") && !strings.Contains(s[openPar+1:closePar], ":") {
			_type = projection
		} else if closePar == len(s)-1 || In(rune(s[closePar+1]), "[+&\\*") {
			_type = limit
		} else {
			openPar = strings.Index(s[openPar+1:], "[")
			continue
		}
		if openPar == 0 {
			switch _type {
			case limit:
				return nil, errors.New("empty limitation")
			default:
				return nil, errors.New("empty projection")
			}
		} else {
			if s[openPar-1] == '>' {
				openLangle := strings.LastIndex(s[:openPar-1], "<")
				if openLangle == -1 {
					return nil, errors.New("used unresolved symbols")
				} else {
					ref, _ := strconv.Atoi(s[openLangle+1 : openPar-1])
					res := SyntaxTree{nil, results[ref], nil, action{_type, "", strings.Split(s[openPar+1:closePar], ",")}}
					results[ref].Parent = &res
					results = append(append(results[:ref], &res), results[ref+1:]...)
					s = s[:openLangle] + fmt.Sprintf("<%d>", len(results)-1) + s[closePar+1:]
				}
			} else if unicode.Is(unicode.Latin, rune(s[openPar-1])) {
				ind := openPar - 2
				for ; ind >= 0 && unicode.Is(unicode.Latin, rune(s[ind])); ind-- {
				}
				variable := SyntaxTree{Data: action{empty, s[ind+1 : openPar], []string{}}}
				res := SyntaxTree{Left: &variable, Data: action{_type, "", strings.Split(s[openPar+1:closePar], ",")}}
				variable.Parent = &res
				results = append(results, &res)
				if ind == -1 {
					s = fmt.Sprintf("<%d>", len(results)-1) + s[closePar+1:]
				} else {
					s = s[:ind+1] + fmt.Sprintf("<%d>", len(results)-1) + s[closePar+1:]
				}
			} else {
				return nil, errors.New("used unresolved symbols")
			}
		}
		openPar = strings.Index(s, "[")
	}

	//finding products, joins, intersections and divisions

	symInd := strings.IndexAny(s, "*[&")
	if symInd == 0 {
		return nil, errors.New("missing an argument of binary operation")
	} else if symInd == -1 {
		symInd = strings.IndexAny(s, "+\\")
	}
	for symInd != -1 {
		endSym := symInd
		if s[symInd] == '[' {
			endSym = strings.Index(s[symInd+1:], "]") + symInd + 1
			if endSym == len(s)-1 {
				return nil, errors.New("missing an argument of binary operation")
			}
		}
		var left, right *SyntaxTree
		var leftBorder, rightBorder int
		if s[endSym+1] == '<' {
			rangle := strings.Index(s[endSym+2:], ">") + endSym + 2
			if rangle == -1 {
				return nil, errors.New("used unresolved symbols")
			}
			ref, err := strconv.Atoi(s[endSym+2 : rangle])
			if err != nil {
				return nil, errors.New("used unresolved symbols")
			}
			right = results[ref]
			rightBorder = rangle
		} else if unicode.Is(unicode.Latin, rune(s[endSym+1])) {
			rightBorder = endSym + 1
			for ; rightBorder < len(s)-1 && unicode.Is(unicode.Latin, rune(s[rightBorder+1])); rightBorder++ {
			}
			robj := SyntaxTree{Data: action{_type: empty, _name: s[endSym+1 : rightBorder+1]}}
			right = &robj
		} else {
			return nil, errors.New("used unresolved symbols")
		}
		if s[symInd-1] == '>' {
			langle := strings.LastIndex(s[:symInd], "<")
			if langle == -1 {
				return nil, errors.New("used unresolved symbols")
			}
			ref, err := strconv.Atoi(s[langle+1 : symInd-1])
			if err != nil {
				return nil, errors.New("used unresolved symbols")
			}
			left = results[ref]
			leftBorder = langle
		} else if unicode.Is(unicode.Latin, rune(s[symInd-1])) {
			leftBorder = symInd - 1
			for ; leftBorder > 0 && unicode.Is(unicode.Latin, rune(s[leftBorder-1])); leftBorder-- {
			}
			lobj := SyntaxTree{Data: action{_type: empty, _name: s[leftBorder:symInd]}}
			left = &lobj
		} else {
			return nil, errors.New("used unresolved symbols")
		}
		var res SyntaxTree
		if symInd == endSym {
			res = SyntaxTree{Left: left, Right: right, Data: action{_type: Designations[s[symInd]]}}
		} else {
			if strings.Contains(s[symInd+1:endSym], ":") {
				half := strings.Split(s[symInd+1:endSym], ":")
				leftHalf := strings.Split(half[0], ",")
				rightHalf := strings.Split(half[1], ",")
				_attrs := append(leftHalf, rightHalf...)
				res = SyntaxTree{Left: left, Right: right, Data: action{_type: division, _attrs: _attrs}}
			} else {
				res = SyntaxTree{Left: left, Right: right, Data: action{_type: join, _attrs: []string{s[symInd+1 : endSym]}}}
			}
		}
		left.Parent = &res
		right.Parent = &res
		results = append(results, &res)
		if leftBorder == 0 {
			s = fmt.Sprintf("<%d>", len(results)-1) + s[rightBorder+1:]
		} else {
			s = s[:leftBorder] + fmt.Sprintf("<%d>", len(results)-1) + s[rightBorder+1:]
		}
		symInd = strings.IndexAny(s, "*[&")
		if symInd == 0 {
			return nil, errors.New("missing an argument of binary operation")
		} else if symInd == -1 {
			symInd = strings.IndexAny(s, "+\\")
		}
	}
	if strings.HasPrefix(s, "<") && strings.HasSuffix(s, ">") {
		ref, err := strconv.Atoi(s[1 : len(s)-1])
		if err != nil {
			return nil, err
		} else {
			return results[ref], nil
		}
	} else {
		for _, c := range s {
			if !unicode.Is(unicode.Latin, rune(c)) {
				return nil, errors.New("query has some extra symbols")
			}
		}
		res := SyntaxTree{Data: action{_name: s}}
		return &res, nil
	}
}
