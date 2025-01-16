package entity

import (
	"encoding/json"
	"fmt"
	"strings"
)

// @Title        ColumnDefinition.go
// @Description
// @Create       david 2024-12-27 18:03
// @Update       david 2024-12-27 18:03

type ColumnDefinition struct {
	Name      string    `json:"name"`
	DataType  DataType  `json:"dataType"`
	IndexType IndexType `json:"indexType"`
}

type IndexType int

const (
	None IndexType = iota
	Primary
	Secondary
)

// 定义字符串映射
var indexTypeNames = map[IndexType]string{
	None:      "None",
	Primary:   "Primary",
	Secondary: "Secondary",
}

var indexTypeValues = map[string]IndexType{
	"None":      None,
	"Primary":   Primary,
	"Secondary": Secondary,
}

// MarshalJSON 实现 json.Marshaler
func (i IndexType) MarshalJSON() ([]byte, error) {
	if s, ok := indexTypeNames[i]; ok {
		return json.Marshal(s)
	}
	return nil, fmt.Errorf("invalid index type: %d", i)
}

// UnmarshalJSON 实现 json.Unmarshaler
func (i *IndexType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if v, ok := indexTypeValues[s]; ok {
		*i = v
		return nil
	}
	return fmt.Errorf("invalid index type string: %s", s)
}

func (c *ColumnDefinition) String() string {
	if c == nil {
		return "<nil>"
	}
	var sb strings.Builder
	sb.WriteString(c.Name)
	sb.WriteString(" ")
	//sb.WriteString(c.Type)
	//
	//if c.Length > 0 {
	//	sb.WriteString(fmt.Sprintf("(%d)", c.Length))
	//}
	//
	//if !c.IsNullable {
	//	sb.WriteString(" NOT NULL")
	//}
	//
	//if c.IsPrimaryKey {
	//	sb.WriteString(" PRIMARY KEY")
	//}

	return sb.String()
}
