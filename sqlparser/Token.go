package sqlparser

import (
	"encoding/json"
	"fmt"
	"strings"
)

// alias type of int
type TokenType int

// otia as enum
const (
	SELECT TokenType = iota
	FROM
	WHERE
	JOIN
	ON
	VALUES
	ORDER_BY
	INSERT_INTO
	CREATE_TABLE
	PRIMARY_KEY
	PARTIAL_KEYWORD
	COMMA
	LEFT_PARENTHESIS
	RIGHT_PARENTHESIS
	IDENTIFIER
	WILDCARD
	INTEGER
	STRING
	INT
	CHAR
	EQUALS
	AND
	IN
	ILLEGAL
	EOF
)

// struct
type Token struct {
	Type  TokenType
	Value string
}

// String implement Stringer interface then you can use fmt.PrintLn() func to print
func (t Token) String() string {
	return fmt.Sprintf("Token{  Type:%v, Value:%v  }", t.Type, t.Value)
}

func newToken(tokenType TokenType, value string) Token {
	return Token{
		Type:  tokenType,
		Value: value,
	}
}
func (t TokenType) String() string {
	switch t {
	case SELECT:
		return "SELECT"
	case FROM:
		return "FROM"
	case WHERE:
		return "WHERE"
	case JOIN:
		return "JOIN"
	case ON:
		return "ON"
	case VALUES:
		return "VALUES"
	case ORDER_BY:
		return "ORDER_BY"
	case INSERT_INTO:
		return "INSERT_INTO"
	case CREATE_TABLE:
		return "CREATE_TABLE"
	case PRIMARY_KEY:
		return "PRIMARY_KEY"
	case PARTIAL_KEYWORD:
		return "PARTIAL_KEYWORD"
	case COMMA:
		return "COMMA"
	case LEFT_PARENTHESIS:
		return "LEFT_PARENTHESIS"
	case RIGHT_PARENTHESIS:
		return "RIGHT_PARENTHESIS"
	case IDENTIFIER:
		return "IDENTIFIER"
	case WILDCARD:
		return "WILDCARD"
	case INTEGER:
		return "INTEGER"
	case STRING:
		return "STRING"
	case INT:
		return "INT"
	case CHAR:
		return "CHAR"
	case EQUALS:
		return "EQUALS"
	case AND:
		return "AND"
	case IN:
		return "IN"
	case ILLEGAL:
		return "ILLEGAL"
	case EOF:
		return "EOF"
	default:
		return fmt.Sprintf("UNKNOWN_TOKEN(%d)", t)
	}
}

// 添加 UnmarshalJSON 方法支持 JSON 反序列化
func (t *TokenType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	// 将字符串转换为大写并去除空格
	s = strings.TrimSpace(strings.ToUpper(s))

	switch s {
	case "INT":
		*t = INT
	case "CHAR":
		*t = CHAR
	case "STRING":
		*t = STRING
	// 如果需要支持其他数据类型，在这里添加
	default:
		return fmt.Errorf("unknown token type: %s", s)
	}

	return nil
}

// 添加 MarshalJSON 方法支持 JSON 序列化
func (t TokenType) MarshalJSON() ([]byte, error) {
	// 只序列化数据类型相关的 Token
	switch t {
	case INT, CHAR, STRING:
		return json.Marshal(t.String())
	default:
		return nil, fmt.Errorf("token type %s cannot be used as data type", t)
	}
}

// 添加辅助函数，用于检查是否是有效的数据类型
func (t TokenType) IsDataType() bool {
	switch t {
	case INT, CHAR, STRING:
		return true
	default:
		return false
	}
}

// 添加解析函数
func ParseDataType(s string) (TokenType, error) {
	switch strings.TrimSpace(strings.ToUpper(s)) {
	case "INT":
		return INT, nil
	case "CHAR":
		return CHAR, nil
	case "STRING":
		return STRING, nil
	default:
		return ILLEGAL, fmt.Errorf("unknown data type: %s", s)
	}
}
