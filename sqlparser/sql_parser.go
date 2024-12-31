package lpjsonparser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// @Title        sql_parser.go
// @Description  parse the tokens into AST nodes
// @Create       2024-12-26 17:16
// @Update       2024-12-26 17:16

type SQLParser struct {
	tokens   []Token // 用 rune 切片存储字符
	position int
}

func NewSQLParser(tokens []Token) *SQLParser {
	return &SQLParser{tokens, 0}
}

func Parse(sql string) (ASTNode, error) {
	lexer := NewLexer(sql)
	tokens := lexer.tokenize()
	sqlparser := NewSQLParser(tokens)
	return sqlparser.parse()
}

func (p *SQLParser) peek() Token {
	if p.position < len(p.tokens) {
		return p.tokens[p.position]
	}
	return Token{EOF, ""}
}

func (p *SQLParser) next() {
	p.position++
}

func (p *SQLParser) consume(typ TokenType) {
	if p.peek().Type == typ {
		p.next()
	} else {
		panic(fmt.Sprintf("expect %v but got %v", typ, p.peek().Type))
	}
}

func (p *SQLParser) match(typ TokenType) bool {
	if p.peek().Type == typ {
		return true
	}
	return false
}

func (p *SQLParser) parse() (ASTNode, error) {
	token := p.peek()
	switch token.Type {
	case SELECT:
		return p.parseSelect(), nil
	case INSERT_INTO:
		return p.parseInsert(), nil
	case CREATE_TABLE:
		return p.parseCreateTable(), nil
	default:
		return nil, fmt.Errorf("unsupported SQL statement: %v", token)
	}
}

// SELECT column1, column2, column3, ... FROM table_name [JOIN table_name ON condition] [WHERE condition] [ORDER BY column1, column2, column3, ...];
func (p *SQLParser) parseSelect() *SelectNode {
	p.consume(SELECT)
	// columnlist parse
	columns := p.parseColumnList()
	p.consume(FROM)
	// tablename parse
	tablename, err := p.parsePlainString()
	if err != nil {

	}
	// join ?
	joins := []*JoinNode{}
	if p.match(JOIN) {
		// join parse
		joins, _ = p.parseJoin()
	}

	wheres := []*BinaryOpNode{}
	if p.match(WHERE) {
		p.next()
		wheres = p.parseWhereCondition()
	}

	orderColumns := []*ColumnNode{}
	if p.match(ORDER_BY) {
		p.next()
		orderColumns = p.parseColumnList()
	}

	return newSelectNode(tablename, columns, wheres, orderColumns, joins)
}

func (p *SQLParser) parseColumnList() []*ColumnNode {
	columnList := []*ColumnNode{}
	if p.match(WILDCARD) {
		columnList = append(columnList, newColumnNode("*", "", WILDCARDN))
	} else {
		for {
			column, err := p.parseColumn()
			if err != nil {

			}
			columnList = append(columnList, column)
			if p.match(COMMA) {
				p.next()
				continue
			} else {
				break
			}
		}
	}
	return columnList
}

func (p *SQLParser) parseColumn() (*ColumnNode, error) {
	if p.match(IDENTIFIER) {
		identifier := p.peek().Value
		p.next()
		if strings.Contains(identifier, ".") {
			parts := strings.Split(identifier, ".")
			return newColumnNode(parts[0], parts[1], TABLE_NAME_PREFIXED), nil
		} else {
			return newColumnNode("", identifier, PLAIN_STRING), nil
		}
	} else {
		return nil, fmt.Errorf("Expected identifier but got %v", p.peek().Type)
	}
}

func (p *SQLParser) parsePlainString() (string, error) {
	if p.match(IDENTIFIER) {
		identifier := p.peek().Value
		p.next()
		return identifier, nil
	} else {
		return "", fmt.Errorf("Expected identifier but got %v", p.peek().Type)
	}
}

func (p *SQLParser) parseJoin() ([]*JoinNode, error) {
	joins := []*JoinNode{}

	for p.match(JOIN) {
		p.next()
		plainString, err := p.parsePlainString()
		if err != nil {
			return nil, err
		}
		p.consume(ON)
		condition, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		joins = append(joins, &JoinNode{plainString, condition})
	}
	return joins, nil
}

func (p *SQLParser) parseExpression() (*BinaryOpNode, error) {
	left, err := p.parseColumnOrLiteralOrSubquery()
	if err != nil {

	}
	if p.match(EQUALS) {
		p.next()
		right, err := p.parseColumnOrLiteralOrSubquery()
		if err != nil {
		}
		node := newBinaryOpNode("=", left, right)
		return node, nil
	} else if p.match(IN) {
		right := p.parseSubquery()
		node := newBinaryOpNode("IN", left, right)
		return node, nil
	} else {
		return nil, fmt.Errorf("Expected = or IN but got %s", p.peek().Type)
	}
}

func (p *SQLParser) parseColumnOrLiteralOrSubquery() (ASTNode, error) {
	if p.peek().Type == IDENTIFIER {
		return p.parseColumn()
	} else if p.match(INTEGER) {
		value, _ := strconv.Atoi(p.peek().Value)
		literal := newLiteralNode(value)
		p.next()
		return literal, nil
	} else if p.match(STRING) {
		literal := &LiteralNode{Value: p.peek().Value}
		p.next()
		return literal, nil
	} else if p.match(LEFT_PARENTHESIS) {
		return p.parseSubquery(), nil
	} else {
		panic("Expected IDENTIFIER, INTEGER, STRING, or subquery")
	}
}

func (p *SQLParser) parseSubquery() *SelectNode {
	p.consume(LEFT_PARENTHESIS)
	subquery := p.parseSelect()
	p.consume(RIGHT_PARENTHESIS)
	return subquery
}

func (p *SQLParser) parseWhereCondition() []*BinaryOpNode {
	conditions := make([]*BinaryOpNode, 0)

	for {
		expression, _ := p.parseExpression()
		conditions = append(conditions, expression)
		if p.match(AND) {
			p.next()
		} else {
			break
		}
	}

	return conditions
}

func (p *SQLParser) parseInsert() ASTNode {
	p.consume(INSERT_INTO)
	tableName, _ := p.parsePlainString()

	var columns []string
	if p.match(LEFT_PARENTHESIS) {
		p.next()
		columns = p.parsePlainStringList()
		p.consume(RIGHT_PARENTHESIS)
	}

	p.consume(VALUES)
	p.consume(LEFT_PARENTHESIS)
	values := p.parseValueList()
	p.consume(RIGHT_PARENTHESIS)

	return &InsertNode{
		TableName: tableName,
		Columns:   columns,
		Values:    values,
	}
}

func (p *SQLParser) parsePlainStringList() []string {
	stringList := make([]string, 0)

	for {
		plainString, _ := p.parsePlainString()
		stringList = append(stringList, plainString)
		if p.match(COMMA) {
			p.next()
			continue
		} else {
			break
		}
	}

	return stringList
}

func (p *SQLParser) parseValueList() []string {
	values := make([]string, 0)

	for {
		if p.match(INTEGER) {
			val := p.peek().Value
			values = append(values, val)
			p.next()
		} else if p.match(STRING) {
			values = append(values, p.peek().Value)
			p.next()
		} else {
			panic("Expected INTEGER or STRING in VALUES clause")
		}

		if p.match(COMMA) {
			p.next()
			continue
		} else {
			break
		}
	}

	return values
}

/*
 * CREATE TABLE table_name (column1 datatype PRIMARY KEY, column2 datatype, ...);
 */
func (p *SQLParser) parseCreateTable() ASTNode {
	p.consume(CREATE_TABLE)
	tableName, _ := p.parsePlainString()
	p.consume(LEFT_PARENTHESIS)
	columns := p.parseColumnDefinitions()
	p.consume(RIGHT_PARENTHESIS)

	return newCreateTbaleNode(tableName, columns)
}

func (p *SQLParser) parseColumnDefinitions() []*ColumnDefinition {
	columns := make([]*ColumnDefinition, 0)

	for {
		columnName, _ := p.parsePlainString()
		dataType, _ := p.parseDataType()

		isPrimaryKey := p.match(PRIMARY_KEY)
		if isPrimaryKey {
			p.next()
		}

		columns = append(columns, &ColumnDefinition{
			Name:       columnName,
			DataType:   dataType,
			PrimaryKey: isPrimaryKey,
		})

		if p.match(COMMA) {
			p.next()
			continue
		} else {
			break
		}
	}

	return columns
}

func (p *SQLParser) parseDataType() (TokenType, error) {
	if p.match(INT) {
		p.next()
		return INT, nil
	} else if p.match(CHAR) {
		p.next()
		return CHAR, nil
	} else {
		return 0, errors.New("unsupported data type")
	}
}