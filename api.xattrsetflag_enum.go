// Code generated by "go-enum -type XAttrSetFlag --linecomment"; DO NOT EDIT.

package webhdfs

import (
	"database/sql"
	"database/sql/driver"
	"encoding"
	"encoding/json"
	"fmt"
	"strconv"
)

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[XAttrSetFlagCreate-0]
	_ = x[XAttrSetFlagReplace-1]
}

const _XAttrSetFlag_name = "\"CREATE\"\"REPLACE\""

var _XAttrSetFlag_index = [...]uint8{0, 8, 17}

func _() {
	var _nil_XAttrSetFlag_value = func() (val XAttrSetFlag) { return }()

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type fmt.Stringer" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ fmt.Stringer = _nil_XAttrSetFlag_value
}

func (i XAttrSetFlag) String() string {
	if i < 0 || i >= XAttrSetFlag(len(_XAttrSetFlag_index)-1) {
		return "XAttrSetFlag(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _XAttrSetFlag_name[_XAttrSetFlag_index[i]:_XAttrSetFlag_index[i+1]]
}

// New returns a pointer to a new addr filled with the XAttrSetFlag value passed in.
func (i XAttrSetFlag) New() *XAttrSetFlag {
	clone := i
	return &clone
}

var _XAttrSetFlag_values = []XAttrSetFlag{0, 1}

var _XAttrSetFlag_name_to_values = map[string]XAttrSetFlag{
	_XAttrSetFlag_name[0:8]:  0,
	_XAttrSetFlag_name[8:17]: 1,
}

// ParseXAttrSetFlagString retrieves an enum value from the enum constants string name.
// Throws an error if the param is not part of the enum.
func ParseXAttrSetFlagString(s string) (XAttrSetFlag, error) {
	if val, ok := _XAttrSetFlag_name_to_values[s]; ok {
		return val, nil
	}
	return 0, fmt.Errorf("%s does not belong to XAttrSetFlag values", s)
}

// XAttrSetFlagValues returns all values of the enum
func XAttrSetFlagValues() []XAttrSetFlag {
	return _XAttrSetFlag_values
}

// IsAXAttrSetFlag returns "true" if the value is listed in the enum definition. "false" otherwise
func (i XAttrSetFlag) Registered() bool {
	for _, v := range _XAttrSetFlag_values {
		if i == v {
			return true
		}
	}
	return false
}

func _() {
	var _nil_XAttrSetFlag_value = func() (val XAttrSetFlag) { return }()

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type encoding.BinaryMarshaler" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ encoding.BinaryMarshaler = &_nil_XAttrSetFlag_value

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type encoding.BinaryUnmarshaler" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ encoding.BinaryUnmarshaler = &_nil_XAttrSetFlag_value
}

// MarshalBinary implements the encoding.BinaryMarshaler interface for XAttrSetFlag
func (i XAttrSetFlag) MarshalBinary() (data []byte, err error) {
	return []byte(i.String()), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface for XAttrSetFlag
func (i *XAttrSetFlag) UnmarshalBinary(data []byte) error {
	var err error
	*i, err = ParseXAttrSetFlagString(string(data))
	return err
}

func _() {
	var _nil_XAttrSetFlag_value = func() (val XAttrSetFlag) { return }()

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type json.Marshaler" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ json.Marshaler = _nil_XAttrSetFlag_value

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type encoding.Unmarshaler" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ json.Unmarshaler = &_nil_XAttrSetFlag_value
}

// MarshalJSON implements the json.Marshaler interface for XAttrSetFlag
func (i XAttrSetFlag) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for XAttrSetFlag
func (i *XAttrSetFlag) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("XAttrSetFlag should be a string, got %s", data)
	}

	var err error
	*i, err = ParseXAttrSetFlagString(s)
	return err
}

func _() {
	var _nil_XAttrSetFlag_value = func() (val XAttrSetFlag) { return }()

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type encoding.TextMarshaler" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ encoding.TextMarshaler = _nil_XAttrSetFlag_value

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type encoding.TextUnmarshaler" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ encoding.TextUnmarshaler = &_nil_XAttrSetFlag_value
}

// MarshalText implements the encoding.TextMarshaler interface for XAttrSetFlag
func (i XAttrSetFlag) MarshalText() ([]byte, error) {
	return []byte(i.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface for XAttrSetFlag
func (i *XAttrSetFlag) UnmarshalText(text []byte) error {
	var err error
	*i, err = ParseXAttrSetFlagString(string(text))
	return err
}

//func _() {
//	var _nil_XAttrSetFlag_value = func() (val XAttrSetFlag) { return }()
//
//	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type yaml.Marshaler" compiler error signifies that the base type have changed.
//	// Re-run the go-enum command to generate them again.
//	var _ yaml.Marshaler = _nil_XAttrSetFlag_value
//
//	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type yaml.Unmarshaler" compiler error signifies that the base type have changed.
//	// Re-run the go-enum command to generate them again.
//	var _ yaml.Unmarshaler = &_nil_XAttrSetFlag_value
//}

// MarshalYAML implements a YAML Marshaler for XAttrSetFlag
func (i XAttrSetFlag) MarshalYAML() (interface{}, error) {
	return i.String(), nil
}

// UnmarshalYAML implements a YAML Unmarshaler for XAttrSetFlag
func (i *XAttrSetFlag) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	var err error
	*i, err = ParseXAttrSetFlagString(s)
	return err
}

func _() {
	var _nil_XAttrSetFlag_value = func() (val XAttrSetFlag) { return }()

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type driver.Valuer" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ driver.Valuer = _nil_XAttrSetFlag_value

	// An "cannot convert XAttrSetFlag literal (type XAttrSetFlag) to type sql.Scanner" compiler error signifies that the base type have changed.
	// Re-run the go-enum command to generate them again.
	var _ sql.Scanner = &_nil_XAttrSetFlag_value
}

func (i XAttrSetFlag) Value() (driver.Value, error) {
	return i.String(), nil
}

func (i *XAttrSetFlag) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	str, ok := value.(string)
	if !ok {
		bytes, ok := value.([]byte)
		if !ok {
			return fmt.Errorf("value is not a byte slice")
		}

		str = string(bytes[:])
	}

	val, err := ParseXAttrSetFlagString(str)
	if err != nil {
		return err
	}

	*i = val
	return nil
}

// XAttrSetFlagSliceContains reports whether sunEnums is within enums.
func XAttrSetFlagSliceContains(enums []XAttrSetFlag, sunEnums ...XAttrSetFlag) bool {
	var seenEnums = map[XAttrSetFlag]bool{}
	for _, e := range sunEnums {
		seenEnums[e] = false
	}

	for _, v := range enums {
		if _, has := seenEnums[v]; has {
			seenEnums[v] = true
		}
	}

	for _, seen := range seenEnums {
		if !seen {
			return false
		}
	}

	return true
}

// XAttrSetFlagSliceContainsAny reports whether any sunEnum is within enums.
func XAttrSetFlagSliceContainsAny(enums []XAttrSetFlag, sunEnums ...XAttrSetFlag) bool {
	var seenEnums = map[XAttrSetFlag]struct{}{}
	for _, e := range sunEnums {
		seenEnums[e] = struct{}{}
	}

	for _, v := range enums {
		if _, has := seenEnums[v]; has {
			return true
		}
	}

	return false
}