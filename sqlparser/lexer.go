package sqlparser

import (
	_ "fmt"
	_ "go/token"
	. "godb/entity"
	"strings"
	_ "unicode"
)

type SQLLexer struct {
	input    []rune // 用 rune 切片存储字符
	position int
	ch       rune // 用 rune 存储当前字符
}

func NewLexer(input string) *SQLLexer {
	l := &SQLLexer{input: []rune(input)}
	l.readChar()
	return l
}

func (l *SQLLexer) tokenize() []Token {
	tokens := make([]Token, 0)
	for {
		token := l.NextToken()
		tokens = append(tokens, token)

		if token.Type == EOF {
			break
		}
	}
	return tokens
}

func (l *SQLLexer) NextToken() Token {
	l.skipWhitespace()

	if l.ch == 0 {
		return NewToken(EOF, "")
	}

	switch l.ch {
	case 0:
		return NewToken(EOF, "")
	case ',':
		l.readChar()
		return NewToken(COMMA, ",")
	case '(':
		l.readChar()
		return NewToken(LEFT_PARENTHESIS, "(")
	case ')':
		l.readChar()
		return NewToken(RIGHT_PARENTHESIS, ")")
	case '=':
		l.readChar()
		return NewToken(EQUALS, "=")
	case '*':
		l.readChar()
		return NewToken(WILDCARD, "*")
	case '\'', '"':
		return l.readString()
	default:
		if isLetter(l.ch) {
			return l.readKeywordOrIdent()
		}
		if isDigit(l.ch) {
			return l.readNumber()
		}
		// 对于无法识别的字符，直接跳过并继续读取下一个字符
		l.readChar()
		return l.NextToken()
	}
}

func (l *SQLLexer) readKeywordOrIdent() Token {
	position := l.position - 1 // 需要减1因为readChar已经前进了一位

	// 读取第一个单词
	for l.ch != 0 && (isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' || l.ch == '.') {
		l.readChar()
	}
	//logger.Debug(l.input[position:l.position])
	//logger.Debug(l.ch)

	// 注意这里不要包含最后的 NULL 字符
	word := strings.TrimSpace(string(l.input[position : l.position-1]))
	//fmt.Printf("word:|%s|\n", word)

	// 先检查是否是复合关键字
	//logger.Debug("space: ", l.peekIsSpace())
	if l.peekIsSpace() {
		//logger.Debug("position: ", l.input[l.position])
		switch strings.ToUpper(word) {
		case "ORDER":
			if l.tryReadNextWord("BY") {
				return NewToken(ORDER_BY, "ORDER BY")
			}
		case "INSERT":
			if l.tryReadNextWord("INTO") {
				return NewToken(INSERT_INTO, "INSERT INTO")
			}
		case "CREATE":
			if l.tryReadNextWord("TABLE") {
				return NewToken(CREATE_TABLE, "CREATE TABLE")
			}
		case "PRIMARY":
			if l.tryReadNextWord("KEY") {
				return NewToken(PRIMARY_KEY, "PRIMARY KEY")
			}
		}
	}

	// 检查单个关键字
	switch strings.ToUpper(word) {
	case "SELECT":
		return NewToken(SELECT, word)
	case "FROM":
		return NewToken(FROM, word)
	case "WHERE":
		return NewToken(WHERE, word)
	case "JOIN":
		return NewToken(JOIN, word)
	case "ON":
		return NewToken(ON, word)
	case "VALUES":
		return NewToken(VALUES, word)
	case "AND":
		return NewToken(AND, word)
	case "IN":
		return NewToken(IN, word)
	case "INT":
		return NewToken(INT, word)
	case "CHAR":
		return NewToken(CHAR, word)
	case "INDEX":
		return NewToken(INDEX, word)
	case "UPDATE":
		return NewToken(UPDATE, word)
	case "SET":
		return NewToken(SET, word)
	default:
		return NewToken(IDENTIFIER, word)
	}
}

// 新增辅助函数
func (l *SQLLexer) peekIsSpace() bool {
	if l.position >= len(l.input) {
		return false
	}
	//fmt.Printf("peek ch:|%s|\n", string(l.ch))
	return l.ch == ' '
}

func (l *SQLLexer) peekWord() string {
	pos := l.position
	for pos < len(l.input) && isLetter(l.input[pos]) {
		pos++
	}
	return string(l.input[l.position:pos])
}

func (l *SQLLexer) readWord() {
	for l.position < len(l.input) && isLetter(l.ch) {
		l.readChar()
	}
}

func (l *SQLLexer) readString() Token {
	quote := l.ch
	position := l.position
	l.readChar()

	for l.ch != quote && l.ch != 0 {
		l.readChar()
	}

	if l.ch == quote {
		str := string(l.input[position : l.position-1])
		l.readChar()
		return NewToken(STRING, str)
	}
	return NewToken(EOF, "")
}

func (l *SQLLexer) readNumber() Token {
	position := l.position - 1

	for isDigit(l.ch) {
		l.readChar()
	}
	// 使用 strings.TrimSpace 来去除可能的空字符
	numberStr := strings.TrimRight(string(l.input[position:l.position-1]), "\x00")
	return NewToken(INTEGER, numberStr)
}

func (l *SQLLexer) readChar() {
	if l.position >= len(l.input) {
		l.ch = 0
	} else {
		// read to ch
		l.ch = l.input[l.position]
	}
	// move forward, so the ch is late of position
	l.position++
}

func (l *SQLLexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func isLetter(ch rune) bool {
	return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ch == '_'
}

func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

// 新增辅助函数，用于尝试读取下一个特定单词
func (l *SQLLexer) tryReadNextWord(expected string) bool {
	savedPosition := l.position
	savedCh := l.ch

	l.skipWhitespace()

	//startPos := l.position
	for l.position < len(l.input) && isLetter(l.ch) {
		l.readChar()
	}

	var word string
	if l.input[l.position-1] == ',' {
		word = strings.TrimSpace(string(l.input[savedPosition : l.position-1]))
	} else {
		word = strings.TrimSpace(string(l.input[savedPosition:l.position]))
	}

	if strings.ToUpper(word) == strings.ToUpper(expected) {
		return true
	}

	l.position = savedPosition
	l.ch = savedCh
	return false
}
