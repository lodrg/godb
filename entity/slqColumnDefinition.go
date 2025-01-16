package entity

// @Title        slqColumnDefinition.go
// @Description
// @Create       david 2024-12-31 15:33
// @Update       david 2024-12-31 15:33

import (
	"fmt"
	"strconv"
)

// the DataType is not TokenType
//type SqlColumnDefinition struct {
//	Name         string              `json:"name"`
//	DataType     sqlparser.TokenType `json:"dataType"`
//	IsPrimaryKey bool                `json:"primaryKey"`
//}

// 列定义结构
type SqlColumnDefinition struct {
	Name         string   `json:"name"`
	DataType     DataType `json:"dataType"`
	IsPrimaryKey bool     `json:"primaryKey"`
	//Length       int      `json:"length,omitempty"`   // 对于 CHAR/VARCHAR 类型的长度
	//IsNullable   bool     `json:"nullable,omitempty"` // 是否允许为空
	//DefaultValue string   `json:"default,omitempty"`  // 默认值
}

func newSqlColumnDefinition(name string, dataType DataType, ispk bool) *SqlColumnDefinition {
	return &SqlColumnDefinition{
		Name:         name,
		DataType:     dataType,
		IsPrimaryKey: ispk,
	}
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
