package sqlparser

import (
	"testing"
)

func TestLexer_BasicTokens(t *testing.T) {
	tests := []struct {
		input    string
		expected Token
	}{
		{"*", Token{Type: WILDCARD, Value: "*"}},
		{",", Token{Type: COMMA, Value: ","}},
		{"(", Token{Type: LEFT_PARENTHESIS, Value: "("}},
		{")", Token{Type: RIGHT_PARENTHESIS, Value: ")"}},
		{"=", Token{Type: EQUALS, Value: "="}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()
			if token != tt.expected {
				t.Errorf("wrong token. got=%+v, want=%+v", token, tt.expected)
			}
		})
	}
}

func TestLexer_Keywords(t *testing.T) {
	tests := []struct {
		input    string
		expected Token
	}{
		{"SELECT", Token{Type: SELECT, Value: "SELECT"}},
		{"FROM", Token{Type: FROM, Value: "FROM"}},
		{"WHERE", Token{Type: WHERE, Value: "WHERE"}},
		{"INSERT INTO", Token{Type: INSERT_INTO, Value: "INSERT INTO"}},
		{"CREATE TABLE", Token{Type: CREATE_TABLE, Value: "CREATE TABLE"}},
		{"ORDER BY", Token{Type: ORDER_BY, Value: "ORDER BY"}},
		{"PRIMARY KEY", Token{Type: PRIMARY_KEY, Value: "PRIMARY KEY"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()
			if token != tt.expected {
				t.Errorf("wrong token. got=%+v, want=%+v", token, tt.expected)
			}
		})
	}
}

func TestLexer_Identifiers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"user_name", "user_name"},
		{"table2", "table2"},
		{"UserName", "UserName"},
		{"_hidden", "_hidden"},
		{"users.id", "users.id"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()
			if token.Type != IDENTIFIER || token.Value != tt.expected {
				t.Errorf("wrong identifier. got=%+v, want=IDENTIFIER(%s)", token, tt.expected)
			}
		})
	}
}

func TestLexer_Literals(t *testing.T) {
	tests := []struct {
		input    string
		expected Token
	}{
		{"'John'", Token{Type: STRING, Value: "John"}},
		{"\"Smith\"", Token{Type: STRING, Value: "Smith"}},
		{"42", Token{Type: INTEGER, Value: "42"}},
		{"123", Token{Type: INTEGER, Value: "123"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()
			if token != tt.expected {
				t.Errorf("wrong literal. got=%+v, want=%+v", token, tt.expected)
			}
		})
	}
}

func TestLexer_CompleteQueries(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "Simple SELECT",
			input: "SELECT * FROM users",
			expected: []Token{
				{Type: SELECT, Value: "SELECT"},
				{Type: WILDCARD, Value: "*"},
				{Type: FROM, Value: "FROM"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: EOF, Value: ""},
			},
		},
		{
			name:  "SELECT with WHERE",
			input: "SELECT id, name FROM users WHERE age = 25",
			expected: []Token{
				{Type: SELECT, Value: "SELECT"},
				{Type: IDENTIFIER, Value: "id"},
				{Type: COMMA, Value: ","},
				{Type: IDENTIFIER, Value: "name"},
				{Type: FROM, Value: "FROM"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: WHERE, Value: "WHERE"},
				{Type: IDENTIFIER, Value: "age"},
				{Type: EQUALS, Value: "="},
				{Type: INTEGER, Value: "25"},
				{Type: EOF, Value: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tokens := lexer.tokenize()

			if len(tokens) != len(tt.expected) {
				t.Fatalf("wrong number of tokens. got=%d, want=%d",
					len(tokens), len(tt.expected))
			}

			for i, token := range tokens {
				if token != tt.expected[i] {
					t.Errorf("token[%d] wrong. got=%+v, want=%+v",
						i, token, tt.expected[i])
				}
			}
		})
	}
}

func TestLexer_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"Unterminated string", "SELECT * FROM users WHERE name = 'John"},
		{"Invalid character", "SELECT @ FROM users"},
		{"Unterminated identifier", "SELECT * FROM users."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tokens := lexer.tokenize()
			lastToken := tokens[len(tokens)-1]

			// 检查是否以 EOF 结束
			if lastToken.Type != EOF {
				t.Errorf("expected EOF token for error case, got %+v", lastToken)
			}
		})
	}
}

func TestLexer_SQLStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "SELECT - Basic",
			input: "SELECT id, name FROM users",
			expected: []Token{
				{Type: SELECT, Value: "SELECT"},
				{Type: IDENTIFIER, Value: "id"},
				{Type: COMMA, Value: ","},
				{Type: IDENTIFIER, Value: "name"},
				{Type: FROM, Value: "FROM"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: EOF, Value: ""},
			},
		},
		{
			name:  "SELECT - With WHERE",
			input: "SELECT * FROM users WHERE age = 25 AND name = 'John'",
			expected: []Token{
				{Type: SELECT, Value: "SELECT"},
				{Type: WILDCARD, Value: "*"},
				{Type: FROM, Value: "FROM"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: WHERE, Value: "WHERE"},
				{Type: IDENTIFIER, Value: "age"},
				{Type: EQUALS, Value: "="},
				{Type: INTEGER, Value: "25"},
				{Type: AND, Value: "AND"},
				{Type: IDENTIFIER, Value: "name"},
				{Type: EQUALS, Value: "="},
				{Type: STRING, Value: "John"},
				{Type: EOF, Value: ""},
			},
		},
		{
			name:  "SELECT - With JOIN",
			input: "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
			expected: []Token{
				{Type: SELECT, Value: "SELECT"},
				{Type: IDENTIFIER, Value: "users.name"},
				{Type: COMMA, Value: ","},
				{Type: IDENTIFIER, Value: "orders.amount"},
				{Type: FROM, Value: "FROM"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: JOIN, Value: "JOIN"},
				{Type: IDENTIFIER, Value: "orders"},
				{Type: ON, Value: "ON"},
				{Type: IDENTIFIER, Value: "users.id"},
				{Type: EQUALS, Value: "="},
				{Type: IDENTIFIER, Value: "orders.user_id"},
				{Type: EOF, Value: ""},
			},
		},
		{
			name:  "SELECT - With ORDER BY",
			input: "SELECT * FROM users ORDER BY created_at",
			expected: []Token{
				{Type: SELECT, Value: "SELECT"},
				{Type: WILDCARD, Value: "*"},
				{Type: FROM, Value: "FROM"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: ORDER_BY, Value: "ORDER BY"},
				{Type: IDENTIFIER, Value: "created_at"},
				{Type: EOF, Value: ""},
			},
		},
		{
			name:  "SELECT - With IN clause",
			input: "SELECT * FROM users WHERE id IN (1, 2, 3)",
			expected: []Token{
				{Type: SELECT, Value: "SELECT"},
				{Type: WILDCARD, Value: "*"},
				{Type: FROM, Value: "FROM"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: WHERE, Value: "WHERE"},
				{Type: IDENTIFIER, Value: "id"},
				{Type: IN, Value: "IN"},
				{Type: LEFT_PARENTHESIS, Value: "("},
				{Type: INTEGER, Value: "1"},
				{Type: COMMA, Value: ","},
				{Type: INTEGER, Value: "2"},
				{Type: COMMA, Value: ","},
				{Type: INTEGER, Value: "3"},
				{Type: RIGHT_PARENTHESIS, Value: ")"},
				{Type: EOF, Value: ""},
			},
		},
		{
			name:  "CREATE TABLE - Basic",
			input: "CREATE TABLE users (id INT, name CHAR)",
			expected: []Token{
				{Type: CREATE_TABLE, Value: "CREATE TABLE"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: LEFT_PARENTHESIS, Value: "("},
				{Type: IDENTIFIER, Value: "id"},
				{Type: INT, Value: "INT"},
				{Type: COMMA, Value: ","},
				{Type: IDENTIFIER, Value: "name"},
				{Type: CHAR, Value: "CHAR"},
				{Type: RIGHT_PARENTHESIS, Value: ")"},
				{Type: EOF, Value: ""},
			},
		},
		{
			name:  "CREATE TABLE - With PRIMARY KEY",
			input: "CREATE TABLE users (id INT PRIMARY KEY, name CHAR)",
			expected: []Token{
				{Type: CREATE_TABLE, Value: "CREATE TABLE"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: LEFT_PARENTHESIS, Value: "("},
				{Type: IDENTIFIER, Value: "id"},
				{Type: INT, Value: "INT"},
				{Type: PRIMARY_KEY, Value: "PRIMARY KEY"},
				{Type: COMMA, Value: ","},
				{Type: IDENTIFIER, Value: "name"},
				{Type: CHAR, Value: "CHAR"},
				{Type: RIGHT_PARENTHESIS, Value: ")"},
				{Type: EOF, Value: ""},
			},
		},
		{
			name:  "INSERT - Basic",
			input: "INSERT INTO users VALUES (1, 'John')",
			expected: []Token{
				{Type: INSERT_INTO, Value: "INSERT INTO"},
				{Type: IDENTIFIER, Value: "users"},
				{Type: VALUES, Value: "VALUES"},
				{Type: LEFT_PARENTHESIS, Value: "("},
				{Type: INTEGER, Value: "1"},
				{Type: COMMA, Value: ","},
				{Type: STRING, Value: "John"},
				{Type: RIGHT_PARENTHESIS, Value: ")"},
				{Type: EOF, Value: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tokens := lexer.tokenize()

			if len(tokens) != len(tt.expected) {
				t.Fatalf("wrong number of tokens.\ngot=%d tokens: %+v\nwant=%d tokens: %+v",
					len(tokens), tokens, len(tt.expected), tt.expected)
			}

			for i, token := range tokens {
				if token != tt.expected[i] {
					t.Errorf("token[%d] wrong.\ngot=%+v\nwant=%+v",
						i, token, tt.expected[i])
				}
			}
		})
	}
}
