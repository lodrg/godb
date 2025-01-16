package entity

import (
	"encoding/json"
	"fmt"
	"strings"
)

// @Title        DataType.go
// @Description
// @Create       david 2025-01-15 10:55
// @Update       david 2025-01-15 10:55
// 数据类型枚举
type DataType int

const (
	TypeUnknown DataType = iota
	TypeInt
	TypeUint
	TypeBigInt
	TypeFloat
	TypeDouble
	TypeChar
	TypeVarchar
	TypeText
	TypeDate
	TypeTimestamp
	TypeBoolean
)

// 为了便于显示和解析
func (dt DataType) String() string {
	switch dt {
	case TypeInt:
		return "INT"
	case TypeUint:
		return "UINT"
	case TypeBigInt:
		return "BIGINT"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeChar:
		return "CHAR"
	case TypeVarchar:
		return "VARCHAR"
	case TypeText:
		return "TEXT"
	case TypeDate:
		return "DATE"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeBoolean:
		return "BOOLEAN"
	default:
		return "UNKNOWN"
	}
}

// 从字符串解析数据类型
func ParseDataType(s string) DataType {
	switch strings.ToUpper(s) {
	case "INT", "INTEGER":
		return TypeInt
	case "UINT", "UNSIGNED INT":
		return TypeUint
	case "BIGINT":
		return TypeBigInt
	case "FLOAT":
		return TypeFloat
	case "DOUBLE":
		return TypeDouble
	case "CHAR":
		return TypeChar
	case "VARCHAR":
		return TypeVarchar
	case "TEXT":
		return TypeText
	case "DATE":
		return TypeDate
	case "TIMESTAMP":
		return TypeTimestamp
	case "BOOLEAN", "BOOL":
		return TypeBoolean
	default:
		return TypeUnknown
	}
}

// MarshalJSON 实现 JSON 序列化
func (dt DataType) MarshalJSON() ([]byte, error) {
	return json.Marshal(dt.String())
}

// UnmarshalJSON 实现 JSON 反序列化
func (dt *DataType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	switch strings.ToUpper(s) {
	case "INT":
		*dt = TypeInt
	case "UINT":
		*dt = TypeUint
	case "FLOAT":
		*dt = TypeFloat
	case "DOUBLE":
		*dt = TypeDouble
	case "CHAR":
		*dt = TypeChar
	case "VARCHAR":
		*dt = TypeVarchar
	case "TEXT":
		*dt = TypeText
	case "BOOLEAN":
		*dt = TypeBoolean
	default:
		return fmt.Errorf("unknown data type: %s", s)
	}

	return nil
}
