package sqlparser

import (
	"godb/entity"
	"testing"
)

func TestLexer_BasicTokens(t *testing.T) {
	tests := []struct {
		input    string
		expected entity.Token
	}{
		{"*", entity.Token{Type: entity.WILDCARD, Value: "*"}},
		{",", entity.Token{Type: entity.COMMA, Value: ","}},
		{"(", entity.Token{Type: entity.LEFT_PARENTHESIS, Value: "("}},
		{")", entity.Token{Type: entity.RIGHT_PARENTHESIS, Value: ")"}},
		{"=", entity.Token{Type: entity.EQUALS, Value: "="}},
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
		expected entity.Token
	}{
		{"SELECT", entity.Token{Type: entity.SELECT, Value: "SELECT"}},
		{"FROM", entity.Token{Type: entity.FROM, Value: "FROM"}},
		{"WHERE", entity.Token{Type: entity.WHERE, Value: "WHERE"}},
		{"INSERT INTO", entity.Token{Type: entity.INSERT_INTO, Value: "INSERT INTO"}},
		{"CREATE TABLE", entity.Token{Type: entity.CREATE_TABLE, Value: "CREATE TABLE"}},
		{"ORDER BY", entity.Token{Type: entity.ORDER_BY, Value: "ORDER BY"}},
		{"PRIMARY KEY", entity.Token{Type: entity.PRIMARY_KEY, Value: "PRIMARY KEY"}},
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
			if token.Type != entity.IDENTIFIER || token.Value != tt.expected {
				t.Errorf("wrong identifier. got=%+v, want=IDENTIFIER(%s)", token, tt.expected)
			}
		})
	}
}

func TestLexer_Literals(t *testing.T) {
	tests := []struct {
		input    string
		expected entity.Token
	}{
		{"'John'", entity.Token{Type: entity.STRING, Value: "John"}},
		{"\"Smith\"", entity.Token{Type: entity.STRING, Value: "Smith"}},
		{"42", entity.Token{Type: entity.INTEGER, Value: "42"}},
		{"123", entity.Token{Type: entity.INTEGER, Value: "123"}},
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
		expected []entity.Token
	}{
		{
			name:  "Simple SELECT",
			input: "SELECT * FROM users",
			expected: []entity.Token{
				{Type: entity.SELECT, Value: "SELECT"},
				{Type: entity.WILDCARD, Value: "*"},
				{Type: entity.FROM, Value: "FROM"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "SELECT with WHERE",
			input: "SELECT id, name FROM users WHERE age = 25",
			expected: []entity.Token{
				{Type: entity.SELECT, Value: "SELECT"},
				{Type: entity.IDENTIFIER, Value: "id"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.IDENTIFIER, Value: "name"},
				{Type: entity.FROM, Value: "FROM"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.WHERE, Value: "WHERE"},
				{Type: entity.IDENTIFIER, Value: "age"},
				{Type: entity.EQUALS, Value: "="},
				{Type: entity.INTEGER, Value: "25"},
				{Type: entity.EOF, Value: ""},
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
		{"Unterminated string", "SELECT * FROM users WHERE name = 'John'"},
		{"Invalid character", "SELECT @ FROM users"},
		{"Unterminated identifier", "SELECT * FROM users."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tokens := lexer.tokenize()
			lastToken := tokens[len(tokens)-1]

			// 检查是否以 EOF 结束
			if lastToken.Type != entity.EOF {
				t.Errorf("expected EOF token for error case, got %+v", lastToken)
			}
		})
	}
}

func TestLexer_SQLStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []entity.Token
	}{
		{
			name:  "SELECT - Basic",
			input: "SELECT id, name FROM users",
			expected: []entity.Token{
				{Type: entity.SELECT, Value: "SELECT"},
				{Type: entity.IDENTIFIER, Value: "id"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.IDENTIFIER, Value: "name"},
				{Type: entity.FROM, Value: "FROM"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "SELECT - With WHERE",
			input: "SELECT * FROM users WHERE age = 25 AND name = 'John'",
			expected: []entity.Token{
				{Type: entity.SELECT, Value: "SELECT"},
				{Type: entity.WILDCARD, Value: "*"},
				{Type: entity.FROM, Value: "FROM"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.WHERE, Value: "WHERE"},
				{Type: entity.IDENTIFIER, Value: "age"},
				{Type: entity.EQUALS, Value: "="},
				{Type: entity.INTEGER, Value: "25"},
				{Type: entity.AND, Value: "AND"},
				{Type: entity.IDENTIFIER, Value: "name"},
				{Type: entity.EQUALS, Value: "="},
				{Type: entity.STRING, Value: "John"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "SELECT - With JOIN",
			input: "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
			expected: []entity.Token{
				{Type: entity.SELECT, Value: "SELECT"},
				{Type: entity.IDENTIFIER, Value: "users.name"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.IDENTIFIER, Value: "orders.amount"},
				{Type: entity.FROM, Value: "FROM"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.JOIN, Value: "JOIN"},
				{Type: entity.IDENTIFIER, Value: "orders"},
				{Type: entity.ON, Value: "ON"},
				{Type: entity.IDENTIFIER, Value: "users.id"},
				{Type: entity.EQUALS, Value: "="},
				{Type: entity.IDENTIFIER, Value: "orders.user_id"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "SELECT - With ORDER BY",
			input: "SELECT * FROM users ORDER BY created_at",
			expected: []entity.Token{
				{Type: entity.SELECT, Value: "SELECT"},
				{Type: entity.WILDCARD, Value: "*"},
				{Type: entity.FROM, Value: "FROM"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.ORDER_BY, Value: "ORDER BY"},
				{Type: entity.IDENTIFIER, Value: "created_at"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "SELECT - With IN clause",
			input: "SELECT * FROM users WHERE id IN (1, 2, 3)",
			expected: []entity.Token{
				{Type: entity.SELECT, Value: "SELECT"},
				{Type: entity.WILDCARD, Value: "*"},
				{Type: entity.FROM, Value: "FROM"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.WHERE, Value: "WHERE"},
				{Type: entity.IDENTIFIER, Value: "id"},
				{Type: entity.IN, Value: "IN"},
				{Type: entity.LEFT_PARENTHESIS, Value: "("},
				{Type: entity.INTEGER, Value: "1"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.INTEGER, Value: "2"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.INTEGER, Value: "3"},
				{Type: entity.RIGHT_PARENTHESIS, Value: ")"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "CREATE TABLE - Basic",
			input: "CREATE TABLE users (id INT, name CHAR)",
			expected: []entity.Token{
				{Type: entity.CREATE_TABLE, Value: "CREATE TABLE"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.LEFT_PARENTHESIS, Value: "("},
				{Type: entity.IDENTIFIER, Value: "id"},
				{Type: entity.INT, Value: "INT"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.IDENTIFIER, Value: "name"},
				{Type: entity.CHAR, Value: "CHAR"},
				{Type: entity.RIGHT_PARENTHESIS, Value: ")"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "CREATE TABLE - With PRIMARY KEY",
			input: "CREATE TABLE users (id INT PRIMARY KEY, name CHAR)",
			expected: []entity.Token{
				{Type: entity.CREATE_TABLE, Value: "CREATE TABLE"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.LEFT_PARENTHESIS, Value: "("},
				{Type: entity.IDENTIFIER, Value: "id"},
				{Type: entity.INT, Value: "INT"},
				{Type: entity.PRIMARY_KEY, Value: "PRIMARY KEY"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.IDENTIFIER, Value: "name"},
				{Type: entity.CHAR, Value: "CHAR"},
				{Type: entity.RIGHT_PARENTHESIS, Value: ")"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "CREATE TABLE - With Sec KEY",
			input: "CREATE TABLE users (id INT PRIMARY KEY, name CHAR INDEX)",
			expected: []entity.Token{
				{Type: entity.CREATE_TABLE, Value: "CREATE TABLE"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.LEFT_PARENTHESIS, Value: "("},
				{Type: entity.IDENTIFIER, Value: "id"},
				{Type: entity.INT, Value: "INT"},
				{Type: entity.PRIMARY_KEY, Value: "PRIMARY KEY"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.IDENTIFIER, Value: "name"},
				{Type: entity.CHAR, Value: "CHAR"},
				{Type: entity.INDEX, Value: "INDEX"},
				{Type: entity.RIGHT_PARENTHESIS, Value: ")"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "INSERT - Basic",
			input: "INSERT INTO users VALUES (1, 'John')",
			expected: []entity.Token{
				{Type: entity.INSERT_INTO, Value: "INSERT INTO"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.VALUES, Value: "VALUES"},
				{Type: entity.LEFT_PARENTHESIS, Value: "("},
				{Type: entity.INTEGER, Value: "1"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.STRING, Value: "John"},
				{Type: entity.RIGHT_PARENTHESIS, Value: ")"},
				{Type: entity.EOF, Value: ""},
			},
		},
		{
			name:  "UPDATE - Basic",
			input: "UPDATE users SET name = 'John', age = 25 WHERE id = 1",
			expected: []entity.Token{
				{Type: entity.UPDATE, Value: "UPDATE"},
				{Type: entity.IDENTIFIER, Value: "users"},
				{Type: entity.SET, Value: "SET"},
				{Type: entity.IDENTIFIER, Value: "name"},
				{Type: entity.EQUALS, Value: "="},
				{Type: entity.STRING, Value: "John"},
				{Type: entity.COMMA, Value: ","},
				{Type: entity.IDENTIFIER, Value: "age"},
				{Type: entity.EQUALS, Value: "="},
				{Type: entity.INTEGER, Value: "25"},
				{Type: entity.WHERE, Value: "WHERE"},
				{Type: entity.IDENTIFIER, Value: "id"},
				{Type: entity.EQUALS, Value: "="},
				{Type: entity.INTEGER, Value: "1"},
				{Type: entity.EOF, Value: ""},
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
