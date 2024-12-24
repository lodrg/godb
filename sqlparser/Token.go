package lpjsonparser

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
