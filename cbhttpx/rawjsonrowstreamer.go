package cbhttpx

import (
	"encoding/json"
	"errors"
)

type rowStreamState int

const (
	rowStreamStateStart    rowStreamState = 0
	rowStreamStateRows     rowStreamState = 1
	rowStreamStatePostRows rowStreamState = 2
	rowStreamStateEnd      rowStreamState = 3
)

type RawJsonRowStreamer struct {
	Decoder    *json.Decoder
	RowsAttrib string

	attribs map[string]json.RawMessage
	state   rowStreamState
}

func (s *RawJsonRowStreamer) begin() error {
	if s.state != rowStreamStateStart {
		return errors.New("unexpected parsing state during begin")
	}

	s.attribs = make(map[string]json.RawMessage)

	// Read the opening { for the result
	t, err := s.Decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return errors.New("expected an opening brace for the result")
	}

	for {
		if !s.Decoder.More() {
			// We reached the end of the object
			s.state = rowStreamStateEnd
			break
		}

		// Read the attribute name
		t, err = s.Decoder.Token()
		if err != nil {
			return err
		}
		key, keyOk := t.(string)
		if !keyOk {
			return errors.New("expected an object property name")
		}

		if key == s.RowsAttrib {
			// Read the opening [ for the rows
			t, err = s.Decoder.Token()
			if err != nil {
				return err
			}

			if t == nil {
				continue
			}

			if delim, ok := t.(json.Delim); !ok || delim != '[' {
				return errors.New("expected an opening bracket for the rows")
			}

			if !s.Decoder.More() {
				// if there are no more rows, immediately move to post-rows
				s.state = rowStreamStatePostRows
				break
			}

			s.state = rowStreamStateRows
			break
		}

		// Read the attribute value
		var value json.RawMessage
		err = s.Decoder.Decode(&value)
		if err != nil {
			return err
		}

		// Save the attribute for the meta-data
		s.attribs[key] = value
	}

	return nil
}

func (s *RawJsonRowStreamer) hasMoreRows() bool {
	if s.state < rowStreamStateRows {
		return false
	}

	// If we've already read all rows or rows is null, we return nil
	if s.state > rowStreamStateRows {
		return false
	}

	return s.Decoder.More()
}

func (s *RawJsonRowStreamer) readRow() (json.RawMessage, error) {
	if s.state < rowStreamStateRows {
		return nil, errors.New("unexpected parsing state during readRow")
	}

	// If we've already read all rows or rows is null, we return nil
	if s.state > rowStreamStateRows {
		return nil, nil
	}

	// Decode this row and return a raw message
	var msg json.RawMessage
	err := s.Decoder.Decode(&msg)
	if err != nil {
		return nil, err
	}

	// If there are no more rows, mark the rows finished and
	// return nil to signal that we are at the end
	if !s.Decoder.More() {
		s.state = rowStreamStatePostRows
	}

	return msg, nil
}

func (s *RawJsonRowStreamer) end() error {
	if s.state < rowStreamStatePostRows {
		return errors.New("unexpected parsing state during end")
	}

	// Check if we've already read everything
	if s.state > rowStreamStatePostRows {
		return nil
	}

	// Read the ending ] for the rows
	t, err := s.Decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		return errors.New("expected an ending bracket for the rows")
	}

	for {
		if !s.Decoder.More() {
			// We reached the end of the object
			s.state = rowStreamStateEnd
			break
		}

		// Read the attribute name
		t, err := s.Decoder.Token()
		if err != nil {
			return err
		}

		key, keyOk := t.(string)
		if !keyOk {
			return errors.New("expected an object property name")
		}

		// Read the attribute value
		var value json.RawMessage
		err = s.Decoder.Decode(&value)
		if err != nil {
			return err
		}

		// Save the attribute for the meta-data
		s.attribs[key] = value
	}

	return nil
}

func (s *RawJsonRowStreamer) ReadPrelude() (json.RawMessage, error) {
	err := s.begin()
	if err != nil {
		return nil, err
	}

	attribsBytes, err := json.Marshal(s.attribs)
	if err != nil {
		return nil, err
	}

	return attribsBytes, nil
}

func (s *RawJsonRowStreamer) HasMoreRows() bool {
	return s.hasMoreRows()
}

func (s *RawJsonRowStreamer) ReadRow() (json.RawMessage, error) {
	return s.readRow()
}

func (s *RawJsonRowStreamer) ReadEpilog() (json.RawMessage, error) {
	err := s.end()
	if err != nil {
		return nil, err
	}

	attribsBytes, err := json.Marshal(s.attribs)
	if err != nil {
		return nil, err
	}

	return attribsBytes, nil
}
