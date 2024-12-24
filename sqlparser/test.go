package lpjsonparser

import "fmt"

func Test() {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
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
	}

	for _, tt := range tests {
		fmt.Printf("\nTesting: %s\n", tt.name)
		fmt.Printf("Input: %s\n", tt.input)

		lexer := NewLexer(tt.input)
		tokens := []Token{}

		// 收集所有token
		for {
			token := lexer.NextToken()
			tokens = append(tokens, token)
			if token.Type == EOF {
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
