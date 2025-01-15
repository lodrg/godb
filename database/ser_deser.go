package database

import (
	"bytes"
	"encoding/binary"
	"godb/logger"
	. "godb/sqlparser"
	"log"
)

// @Title        ser_deser.go
// @Description
// @Create       david 2025-01-15 10:23
// @Update       david 2025-01-15 10:23

func (b *DataBase) serializeRow(record map[string]interface{}, definition *SqlTableDefinition) *bytes.Buffer {
	buf := new(bytes.Buffer)

	for _, column := range definition.Columns {
		switch column.DataType {
		case TypeInt:
			// 写入整数，固定4字节
			ser_Int(record, column, buf)
		case TypeChar:
			// 写入字符串，固定长度(CHAR_SIZE + CHAR_LENGTH)
			ser_Char(record, column, buf)
		default:
			log.Fatal("SerializeRow Unknown column type:", column.DataType)
		}
	}

	return buf
}

func ser_Char(record map[string]interface{}, column SqlColumnDefinition, buf *bytes.Buffer) {
	value := record[column.Name].(string)
	// 创建固定长度的字节数组
	data := make([]byte, CHAR_SIZE+CHAR_LENGTH)

	// 计算实际长度，如果超过CHAR_LENGTH则截断
	strLen := len(value)
	if strLen > CHAR_LENGTH {
		strLen = CHAR_LENGTH
	}

	// 写入长度信息到前CHAR_SIZE字节
	binary.LittleEndian.PutUint32(data[:CHAR_SIZE], uint32(strLen))

	// 复制字符串内容到CHAR_SIZE之后的位置
	copy(data[CHAR_SIZE:], []byte(value))

	buf.Write(data)
}

func ser_Int(record map[string]interface{}, column SqlColumnDefinition, buf *bytes.Buffer) {
	value := record[column.Name].(uint32) // 类型断言
	data := make([]byte, INT_SIZE)
	binary.BigEndian.PutUint32(data, value)
	buf.Write(data)
}

func deserializeRow(definition *SqlTableDefinition, bytes []byte) map[string]interface{} {
	// check row size
	rowSize := getRowSize(definition)
	if rowSize < len(bytes) {
		log.Fatalf("Row size mismatch, row size: %d, expected row size: %d", len(bytes), rowSize)
	}

	// from bytes to typed data
	result := make(map[string]interface{})
	columns := definition.Columns
	curPosition := 0

	for _, column := range columns {
		switch column.DataType {
		case TypeInt:
			// 将4个字节转换为uint32

			curPosition = deser_Int(curPosition, bytes, result, column)

		case TypeChar:
			// 处理字符串类型，去除空字节

			curPosition = deser_Char(curPosition, bytes, result, column)

		default:
			log.Fatal("DeserializeRow Unknown column type:", column.DataType)
		}
	}
	return result
}

func deser_Int(curPosition int, bytes []byte, result map[string]interface{}, column SqlColumnDefinition) int {
	if curPosition+INT_SIZE <= len(bytes) {
		value := binary.BigEndian.Uint32(bytes[curPosition : curPosition+INT_SIZE])
		logger.Debug("value: %x \n", value)
		result[column.Name] = value
		curPosition += INT_SIZE
	}
	return curPosition
}

// 02 00 00 00    31 32 00 00 00 00 00 00
// └─长度信息(4字节)┘└─实际内容(8字节)─────┘
// 值= 2        "12" + 填充的0
func deser_Char(curPosition int, bytes []byte, result map[string]interface{}, column SqlColumnDefinition) int {
	if curPosition+CHAR_SIZE+CHAR_LENGTH <= len(bytes) {
		strBytes := bytes[curPosition : curPosition+CHAR_SIZE+CHAR_LENGTH]

		// 读取长度信息（前CHAR_SIZE字节）
		actualLength := uint32(0)
		actualLength = binary.LittleEndian.Uint32(strBytes[:CHAR_SIZE])

		// 使用实际长度读取内容
		contentBytes := strBytes[CHAR_SIZE:]
		if actualLength > 0 && actualLength <= CHAR_LENGTH {
			result[column.Name] = string(contentBytes[:actualLength])
		} else {
			result[column.Name] = ""
		}

		curPosition += CHAR_SIZE + CHAR_LENGTH
	}
	return curPosition
}
