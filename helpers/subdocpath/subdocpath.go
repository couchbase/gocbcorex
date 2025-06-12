package subdocpath

import (
	"errors"
	"fmt"
	"strconv"
)

type Element struct {
	Key   string
	Index int
}

func Parse(path string) ([]Element, error) {
	if len(path) == 0 {
		return nil, nil
	}

	var parts []Element
	pathStr := ""

	for chIdx := 0; chIdx < len(path); chIdx++ {
		ch := path[chIdx]

		if ch == '.' {
			if pathStr != "" {
				// we already validate that there is an identifier after a period
				// so we don't need to enforce a non-blank path, since there are cases
				// when there is not a path (after an array index)
				parts = append(parts, Element{
					Key: pathStr,
				})
				pathStr = ""
			}

			// check that there is at least one character coming
			if len(path) <= chIdx+1 {
				return nil, errors.New("unexpected end of string after period")
			}

			nextCh := path[chIdx+1]
			switch nextCh {
			case '.':
				return nil, errors.New("unexpected period after period")
			case '[':
				return nil, errors.New("unexpected array start after a period")
			case ']':
				return nil, errors.New("unexpected end of array without start of array")
			}
		} else if ch == '`' {
			chIdx++
			for ; ; chIdx++ {
				if chIdx >= len(path) {
					return nil, errors.New("unexpected end of string during path literal parsing")
				}

				ch := path[chIdx]
				if ch == '`' {
					isEscapedBacktick := false
					if chIdx+1 < len(path) {
						nextCh := path[chIdx+1]
						if nextCh == '`' {
							isEscapedBacktick = true
							chIdx++
						}
					}

					if isEscapedBacktick {
						pathStr += "`"
					} else {
						break
					}
				} else {
					pathStr += string(ch)
				}
			}
		} else if ch == '[' {
			if pathStr != "" {
				parts = append(parts, Element{
					Key: pathStr,
				})
				pathStr = ""
			}

			chIdx++
			for ; ; chIdx++ {
				if chIdx >= len(path) {
					return nil, errors.New("unexpected end of string during array element parsing")
				}

				ch := path[chIdx]
				if ch == '.' {
					return nil, errors.New("unexpected object delimiter inside array indexing")
				} else if ch == '[' {
					return nil, errors.New("unexpected array start inside array indexing")
				} else if ch == ']' {
					v, err := strconv.ParseInt(pathStr, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("invalid array index value: %s", err.Error())
					}

					parts = append(parts, Element{
						Index: int(v),
					})
					pathStr = ""

					break
				} else {
					pathStr += string(ch)
				}
			}
		} else if ch == ']' {
			return nil, errors.New("unexpected end of array without start of array")
		} else {
			pathStr += string(ch)
		}
	}

	if pathStr != "" {
		parts = append(parts, Element{
			Key: pathStr,
		})
	}

	return parts, nil
}
