package sqlparser

import (
	"strings"
)

// @Title        ColumnDefinition.go
// @Description
// @Create       david 2024-12-27 18:03
// @Update       david 2024-12-27 18:03

type ColumnDefinition struct {
	Name       string
	DataType   TokenType
	PrimaryKey bool
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
