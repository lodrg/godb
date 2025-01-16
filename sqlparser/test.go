package sqlparser

import (
	"fmt"
	"godb/entity"
)

func Test() {
	tests := []struct {
		name     string
		input    string
		expected []entity.Token
	}{
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
	}

	for _, tt := range tests {
		fmt.Printf("\nTesting: %s\n", tt.name)
		fmt.Printf("Input: %s\n", tt.input)

		lexer := NewLexer(tt.input)
		tokens := []entity.Token{}

		// 收集所有token
		for {
			token := lexer.NextToken()
			tokens = append(tokens, token)
			if token.Type == entity.EOF {
				break
			}
		}

		// 比较token数量
		if len(tokens) != len(tt.expected) {
			fmt.Printf("❌ Error - token count mismatch: got %d tokens, expected %d tokens\n",
				len(tokens), len(tt.expected))
			continue
		}

		// 逐个比较token
		failed := false
		for i, expectedToken := range tt.expected {
			if tokens[i] != expectedToken {
				fmt.Printf("❌ Error at position %d:\n", i)
				fmt.Printf("  Got: {Type: %v, Value: %q}\n", tokens[i].Type, tokens[i].Value)
				fmt.Printf("  Expected: {Type: %v, Value: %q}\n", expectedToken.Type, expectedToken.Value)
				failed = true
			}
		}

		if !failed {
			fmt.Printf("✅ Success - All tokens match\n")
			for i, token := range tokens {
				fmt.Printf("  Token %d: {Type: %v, Value: %q}\n", i, token.Type, token.Value)
			}
		}
	}
}
