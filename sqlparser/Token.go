package sqlparser

import "fmt"

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
