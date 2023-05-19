package subdocprojection

import (
	"encoding/json"
	"errors"

	"github.com/couchbase/gocbcorex/helpers/subdocpath"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var (
	ErrMixedUsage   = errors.New("mixed usage")
	ErrNonJson      = errors.New("non json")
	ErrPathMismatch = errors.New("path mismatch")
	ErrDuplicateSet = errors.New("duplicate set")
	ErrOutOfBounds  = errors.New("out of bounds")
)

type projectorArrayMap map[int]interface{}

func (a projectorArrayMap) MarshalJSON() ([]byte, error) {
	keys := maps.Keys(a)
	slices.Sort(keys)
	out := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		out = append(out, a[key])
	}
	return json.Marshal(out)
}

type Projector struct {
	data interface{}
}

func (b *Projector) Init(data []byte) error {
	err := json.Unmarshal(data, &b.data)
	if err != nil {
		return ErrNonJson
	}

	return nil
}

func (b *Projector) Get(path []subdocpath.Element) ([]byte, error) {
	if len(path) == 0 {
		return json.Marshal(b.data)
	}

	_, isJson := b.data.(json.RawMessage)
	if isJson {
		// we don't support mixing Getting and Setting on a single Projector
		// object at the moment, so just return an error for now.
		return nil, ErrMixedUsage
	}

	myPath := path[0]
	if myPath.Key == "" {
		// array index
		arrData, ok := b.data.([]interface{})
		if !ok {
			return nil, ErrPathMismatch
		}

		if myPath.Index < 0 || myPath.Index >= len(arrData) {
			return nil, ErrOutOfBounds
		}

		elemData := arrData[myPath.Index]
		elemProj := Projector{elemData}
		return elemProj.Get(path[1:])
	} else {
		// map key
		mapData, ok := b.data.(map[string]interface{})
		if !ok {
			return nil, ErrPathMismatch
		}

		elemData, ok := mapData[myPath.Key]
		if !ok {
			return nil, ErrPathMismatch
		}

		elemProj := Projector{elemData}
		return elemProj.Get(path[1:])
	}
}

func (b *Projector) Set(path []subdocpath.Element, content []byte) error {
	if len(path) == 0 {
		if b.data != nil {
			return ErrDuplicateSet
		}

		b.data = json.RawMessage(content)
		return nil
	}

	_, isJson := b.data.(json.RawMessage)
	if isJson {
		// we've previously set an entire json blob here, and now we are
		// trying to recursively enter that data which is not allowed
		return ErrDuplicateSet
	}

	myPath := path[0]
	if myPath.Key == "" {
		// array index
		var arrData projectorArrayMap

		if b.data == nil {
			arrData = projectorArrayMap(make(map[int]interface{}))
		} else if readArrData, ok := b.data.([]interface{}); ok {
			arrData = projectorArrayMap(make(map[int]interface{}))
			for i, v := range readArrData {
				arrData[i] = v
			}
		} else if readArrData, ok := b.data.(projectorArrayMap); ok {
			arrData = readArrData
		} else {
			return ErrPathMismatch
		}

		subData := Projector{
			data: arrData[myPath.Index],
		}
		subData.Set(path[1:], content)

		arrData[myPath.Index] = subData.data
		b.data = arrData

		return nil
	} else {
		// map key
		var mapData map[string]interface{}

		if b.data == nil {
			mapData = make(map[string]interface{})
		} else {
			readMapData, ok := b.data.(map[string]interface{})
			if !ok {
				return ErrPathMismatch
			}

			mapData = readMapData
		}

		subData := Projector{
			data: mapData[myPath.Key],
		}
		subData.Set(path[1:], content)

		mapData[myPath.Key] = subData.data
		b.data = mapData

		return nil
	}
}

func (b *Projector) Build() ([]byte, error) {
	bytes, err := json.Marshal(b.data)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
