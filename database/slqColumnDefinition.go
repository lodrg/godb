package database

// @Title        slqColumnDefinition.go
// @Description
// @Create       david 2024-12-31 15:33
// @Update       david 2024-12-31 15:33

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// the DataType is not TokenType
//type SqlColumnDefinition struct {
//	Name         string              `json:"name"`
//	DataType     sqlparser.TokenType `json:"dataType"`
//	IsPrimaryKey bool                `json:"primaryKey"`
//}

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

// 列定义结构
type SqlColumnDefinition struct {
	Name         string   `json:"name"`
	DataType     DataType `json:"dataType"`
	IsPrimaryKey bool     `json:"primaryKey"`
	//Length       int      `json:"length,omitempty"`   // 对于 CHAR/VARCHAR 类型的长度
	//IsNullable   bool     `json:"nullable,omitempty"` // 是否允许为空
	//DefaultValue string   `json:"default,omitempty"`  // 默认值
}

// 用于验证值是否符合数据类型
func (def *SqlColumnDefinition) ValidateValue(value string) error {
	//if value == "" && !def.IsNullable {
	//	return fmt.Errorf("column %s does not allow null values", def.Name)
	//}

	switch def.DataType {
	case TypeInt, TypeUint, TypeBigInt:
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return fmt.Errorf("column %s expects integer value, got %s", def.Name, value)
		}
	case TypeFloat, TypeDouble:
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return fmt.Errorf("column %s expects float value, got %s", def.Name, value)
		}
	//case TypeChar, TypeVarchar:
	//	value = strings.Trim(value, "'\"")
	//	if len(value) > def.Length {
	//		return fmt.Errorf("value length %d exceeds column %s max length %d",
	//			len(value), def.Name, def.Length)
	//	}
	case TypeBoolean:
		if _, err := strconv.ParseBool(value); err != nil {
			return fmt.Errorf("column %s expects boolean value, got %s", def.Name, value)
		}
	}
	return nil
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
