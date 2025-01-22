package sqlparser

import (
	"fmt"
	"godb/entity"
	"reflect"
	"strings"
	"testing"
)

// @Title        parser_test.go
// @Description
// @Create       david 2024-12-27 18:13
// @Update       david 2024-12-27 18:13
// 辅助函数：打印 AST 节点的详细信息
func diffNode(got, want entity.ASTNode, path string) string {
	if got == nil && want == nil {
		return ""
	}
	if got == nil {
		return fmt.Sprintf("%s: got nil, want %+v", path, want)
	}
	if want == nil {
		return fmt.Sprintf("%s: got %+v, want nil", path, got)
	}

	var diffs []string

	switch w := want.(type) {
	case *entity.SelectNode:
		g, ok := got.(*entity.SelectNode)
		if !ok {
			return fmt.Sprintf("%s: type mismatch: got %T, want SelectNode", path, got)
		}

		// 打印完整的节点信息，帮助调试
		fmt.Printf("Got SelectNode: %+v\n", g)
		fmt.Printf("Want SelectNode: %+v\n", w)

		if g.TableName != w.TableName {
			diffs = append(diffs, fmt.Sprintf("%s.TableName: got %q, want %q", path, g.TableName, w.TableName))
		}

		// 比较 Columns
		if len(g.Columns) != len(w.Columns) {
			diffs = append(diffs, fmt.Sprintf("%s.Columns: length mismatch: got %d, want %d", path, len(g.Columns), len(w.Columns)))
		} else {
			for i := range w.Columns {
				if d := diffNode(g.Columns[i], w.Columns[i], fmt.Sprintf("%s.Columns[%d]", path, i)); d != "" {
					diffs = append(diffs, d)
				}
			}
		}

		// 比较 WhereClause
		if len(g.WhereClause) != len(w.WhereClause) {
			diffs = append(diffs, fmt.Sprintf("%s.WhereClause: length mismatch: got %d, want %d", path, len(g.WhereClause), len(w.WhereClause)))
		} else {
			for i := range w.WhereClause {
				if d := diffNode(g.WhereClause[i], w.WhereClause[i], fmt.Sprintf("%s.WhereClause[%d]", path, i)); d != "" {
					diffs = append(diffs, d)
				}
			}
		}

		// 比较 Join
		if len(g.Join) != len(w.Join) {
			diffs = append(diffs, fmt.Sprintf("%s.Join: length mismatch: got %d, want %d", path, len(g.Join), len(w.Join)))
		} else {
			for i := range w.Join {
				if d := diffNode(g.Join[i], w.Join[i], fmt.Sprintf("%s.Join[%d]", path, i)); d != "" {
					diffs = append(diffs, d)
				}
			}
		}

		// 比较 OrderByColumns
		if len(g.OrderByColumns) != len(w.OrderByColumns) {
			diffs = append(diffs, fmt.Sprintf("%s.OrderByColumns: length mismatch: got %d, want %d", path, len(g.OrderByColumns), len(w.OrderByColumns)))
		} else {
			for i := range w.OrderByColumns {
				if d := diffNode(g.OrderByColumns[i], w.OrderByColumns[i], fmt.Sprintf("%s.OrderByColumns[%d]", path, i)); d != "" {
					diffs = append(diffs, d)
				}
			}
		}

	case *entity.ColumnNode:
		g, ok := got.(*entity.ColumnNode)
		if !ok {
			return fmt.Sprintf("%s: type mismatch: got %T, want ColumnNode", path, got)
		}
		if g.TableName != w.TableName {
			diffs = append(diffs, fmt.Sprintf("%s.TableName: got %q, want %q", path, g.TableName, w.TableName))
		}
		if g.ColumnName != w.ColumnName {
			diffs = append(diffs, fmt.Sprintf("%s.ColumnName: got %q, want %q", path, g.ColumnName, w.ColumnName))
		}
		if g.ColumnType != w.ColumnType {
			diffs = append(diffs, fmt.Sprintf("%s.ColumnType: got %v, want %v", path, g.ColumnType, w.ColumnType))
		}

	case *entity.BinaryOpNode:
		g, ok := got.(*entity.BinaryOpNode)
		if !ok {
			return fmt.Sprintf("%s: type mismatch: got %T, want BinaryOpNode", path, got)
		}
		if g.Operator != w.Operator {
			diffs = append(diffs, fmt.Sprintf("%s.Operator: got %q, want %q", path, g.Operator, w.Operator))
		}
		if d := diffNode(g.Left, w.Left, path+".Left"); d != "" {
			diffs = append(diffs, d)
		}
		if d := diffNode(g.Right, w.Right, path+".Right"); d != "" {
			diffs = append(diffs, d)
		}

	case *entity.JoinNode:
		g, ok := got.(*entity.JoinNode)
		if !ok {
			return fmt.Sprintf("%s: type mismatch: got %T, want JoinNode", path, got)
		}
		if g.TableName != w.TableName {
			diffs = append(diffs, fmt.Sprintf("%s.TableName: got %q, want %q", path, g.TableName, w.TableName))
		}
		if d := diffNode(g.Condition, w.Condition, path+".Condition"); d != "" {
			diffs = append(diffs, d)
		}

	case *entity.LiteralNode:
		g, ok := got.(*entity.LiteralNode)
		if !ok {
			return fmt.Sprintf("%s: type mismatch: got %T, want LiteralNode", path, got)
		}
		if !reflect.DeepEqual(g.Value, w.Value) {
			diffs = append(diffs, fmt.Sprintf("%s.Value: got %v, want %v", path, g.Value, w.Value))
		}
	}

	return strings.Join(diffs, "\n")
}

func TestSQLParser_Parse(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    entity.ASTNode
		wantErr bool
	}{
		{
			name: "simple select",
			sql:  "SELECT id, name FROM users",
			want: &entity.SelectNode{
				TableName: "users",
				Columns: []*entity.ColumnNode{
					entity.NewColumnNode("", "id", entity.PLAIN_STRING),
					entity.NewColumnNode("", "name", entity.PLAIN_STRING),
				},
				WhereClause:    nil,
				OrderByColumns: nil,
				Join:           nil,
			},
			wantErr: false,
		},
		{
			name: "select with where",
			sql:  "SELECT id FROM users WHERE id = 1",
			want: &entity.SelectNode{
				TableName: "users",
				Columns: []*entity.ColumnNode{
					entity.NewColumnNode("", "id", entity.PLAIN_STRING),
				},
				WhereClause: []*entity.BinaryOpNode{
					entity.NewBinaryOpNode(entity.EQUALS,
						entity.NewColumnNode("", "id", entity.PLAIN_STRING),
						entity.NewLiteralNode(1),
					),
				},
				OrderByColumns: nil,
				Join:           nil,
			},
			wantErr: false,
		},
		{
			name: "select with join",
			sql:  "SELECT users.id, departments.name FROM users JOIN departments ON users.dept_id = departments.id",
			want: &entity.SelectNode{
				TableName: "users",
				Columns: []*entity.ColumnNode{
					entity.NewColumnNode("users", "id", entity.TABLE_NAME_PREFIXED),
					entity.NewColumnNode("departments", "name", entity.TABLE_NAME_PREFIXED),
				},
				Join: []*entity.JoinNode{
					entity.NewJoinNode("departments",
						entity.NewBinaryOpNode(entity.EQUALS,
							entity.NewColumnNode("users", "dept_id", entity.TABLE_NAME_PREFIXED),
							entity.NewColumnNode("departments", "id", entity.TABLE_NAME_PREFIXED),
						),
					),
				},
			},
			wantErr: false,
		},
		{
			name: "select with order by",
			sql:  "SELECT id, name FROM users ORDER BY name",
			want: &entity.SelectNode{
				TableName: "users",
				Columns: []*entity.ColumnNode{
					entity.NewColumnNode("", "id", entity.PLAIN_STRING),
					entity.NewColumnNode("", "name", entity.PLAIN_STRING),
				},
				OrderByColumns: []*entity.ColumnNode{
					entity.NewColumnNode("", "name", entity.PLAIN_STRING),
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if diff := diffNode(got, tt.want, "root"); diff != "" {
					t.Errorf("Parse() differences:\n%s", diff)
				}
			}
		})
	}
}

func TestASTNodeCreation(t *testing.T) {
	t.Run("test binary op node", func(t *testing.T) {
		left := entity.NewColumnNode("", "id", entity.PLAIN_STRING)
		right := entity.NewLiteralNode(1)
		node := entity.NewBinaryOpNode(entity.EQUALS, left, right)

		if node.Operator != entity.EQUALS {
			t.Errorf("Expected operator '=', got %s", node.Operator)
		}
		if !reflect.DeepEqual(node.Left, left) {
			t.Errorf("Left node mismatch")
		}
		if !reflect.DeepEqual(node.Right, right) {
			t.Errorf("Right node mismatch")
		}
	})

	t.Run("test column node", func(t *testing.T) {
		node := entity.NewColumnNode("users", "id", entity.TABLE_NAME_PREFIXED)

		if node.TableName != "users" {
			t.Errorf("Expected table name 'users', got %s", node.TableName)
		}
		if node.ColumnName != "id" {
			t.Errorf("Expected column name 'id', got %s", node.ColumnName)
		}
		if node.ColumnType != entity.TABLE_NAME_PREFIXED {
			t.Errorf("Expected column type TABLE_NAME_PREFIXED, got %v", node.ColumnType)
		}
	})

	t.Run("test select node", func(t *testing.T) {
		columns := []*entity.ColumnNode{entity.NewColumnNode("", "id", entity.PLAIN_STRING)}
		whereClause := []*entity.BinaryOpNode{
			entity.NewBinaryOpNode(entity.EQUALS,
				entity.NewColumnNode("", "id", entity.PLAIN_STRING),
				entity.NewLiteralNode(1),
			),
		}
		node := entity.NewSelectNode("users", columns, whereClause, nil, nil)

		if node.TableName != "users" {
			t.Errorf("Expected table name 'users', got %s", node.TableName)
		}
		if len(node.Columns) != 1 {
			t.Errorf("Expected 1 column, got %d", len(node.Columns))
		}
		if len(node.WhereClause) != 1 {
			t.Errorf("Expected 1 where clause, got %d", len(node.WhereClause))
		}
	})
}

// 测试 CreateTableNode
func TestCreateTableNode(t *testing.T) {
	columns := []*entity.ColumnDefinition{
		{Name: "id", DataType: entity.TypeInt, IndexType: entity.Primary},
		{Name: "name", DataType: entity.TypeChar, IndexType: entity.None},
	}

	node := entity.NewCreateTableNode("users", columns)

	if node.TableName != "users" {
		t.Errorf("Expected table name 'users', got %s", node.TableName)
	}

	if len(node.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(node.Columns))
	}

	if !(node.Columns[0].IndexType == entity.Primary) {
		t.Error("Expected first column to be primary key")
	}
}

// 测试 CreateTableSecindex
func TestCreateTableSecindex(t *testing.T) {
	columns := []*entity.ColumnDefinition{
		{Name: "id", DataType: entity.TypeInt, IndexType: entity.Primary},
		{Name: "name", DataType: entity.TypeChar, IndexType: entity.Secondary},
	}

	node := entity.NewCreateTableNode("users", columns)

	if node.TableName != "users" {
		t.Errorf("Expected table name 'users', got %s", node.TableName)
	}

	if len(node.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(node.Columns))
	}

	if !(node.Columns[0].IndexType == entity.Primary) {
		t.Error("Expected first column to be primary key")
	}
	if !(node.Columns[1].IndexType == entity.Secondary) {
		t.Error("Expected first column to be Secondary key")
	}
}

// 边界情况测试
func TestEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "empty SQL",
			sql:     "",
			wantErr: true,
		},
		{
			name:    "select without columns",
			sql:     "SELECT FROM users",
			wantErr: true,
		},
		{
			name:    "select without table",
			sql:     "SELECT id",
			wantErr: true,
		},
		{
			name:    "incomplete where clause",
			sql:     "SELECT id FROM users WHERE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// 基准测试
func BenchmarkParse(b *testing.B) {
	sql := "SELECT users.id, departments.name FROM users JOIN departments ON users.dept_id = departments.id WHERE users.age > 18 ORDER BY users.name"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSQLParser_Parse_Insert(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    entity.ASTNode
		wantErr bool
	}{
		{
			name: "simple insert",
			sql:  "INSERT INTO users (id, name) VALUES (1, 'david')",
			want: &entity.InsertNode{
				TableName: "users",
				Columns:   []string{"id", "name"},
				Values:    []interface{}{"1", "david"},
			},
			wantErr: false,
		},
		{
			name: "insert without columns",
			sql:  "INSERT INTO users VALUES (1, 'david')",
			want: &entity.InsertNode{
				TableName: "users",
				Columns:   []string{},
				Values:    []interface{}{"1", "david"},
			},
			wantErr: false,
		},
		{
			name: "insert multiple values",
			sql:  "INSERT INTO users (id, name, age) VALUES (1, 'david', 25)",
			want: &entity.InsertNode{
				TableName: "users",
				Columns:   []string{"id", "name", "age"},
				Values:    []interface{}{"1", "david", "25"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				gotNode, ok := got.(*entity.InsertNode)
				if !ok {
					t.Errorf("Parse() got = %T, want *InsertNode", got)
					return
				}
				if gotNode.TableName != tt.want.(*entity.InsertNode).TableName {
					t.Errorf("TableName = %v, want %v", gotNode.TableName, tt.want.(*entity.InsertNode).TableName)
				}
				if !reflect.DeepEqual(gotNode.Columns, tt.want.(*entity.InsertNode).Columns) {
					t.Errorf("Columns = %v, want %v", gotNode.Columns, tt.want.(*entity.InsertNode).Columns)
				}
				if !reflect.DeepEqual(gotNode.Values, tt.want.(*entity.InsertNode).Values) {
					t.Errorf("Values = %v, want %v", gotNode.Values, tt.want.(*entity.InsertNode).Values)
				}
			}
		})
	}
}

func TestSQLParser_Parse_CreateTable(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    entity.ASTNode
		wantErr bool
	}{
		{
			name: "create table with primary key",
			sql:  "CREATE TABLE users (id INT PRIMARY KEY, name CHAR)",
			want: &entity.CreateTableNode{
				TableName: "users",
				Columns: []*entity.ColumnDefinition{
					{Name: "id", DataType: entity.TypeInt, IndexType: entity.Primary},
					{Name: "name", DataType: entity.TypeChar, IndexType: entity.None},
				},
			},
			wantErr: false,
		},
		{
			name: "create table without primary key",
			sql:  "CREATE TABLE departments (id INT, name CHAR)",
			want: &entity.CreateTableNode{
				TableName: "departments",
				Columns: []*entity.ColumnDefinition{
					{Name: "id", DataType: entity.TypeInt, IndexType: entity.None},
					{Name: "name", DataType: entity.TypeChar, IndexType: entity.None},
				},
			},
			wantErr: false,
		},
		{
			name: "create table with multiple columns",
			sql:  "CREATE TABLE employees (id INT PRIMARY KEY, name CHAR, age INT, dept_id INT)",
			want: &entity.CreateTableNode{
				TableName: "employees",
				Columns: []*entity.ColumnDefinition{
					{Name: "id", DataType: entity.TypeInt, IndexType: entity.Primary},
					{Name: "name", DataType: entity.TypeChar, IndexType: entity.None},
					{Name: "age", DataType: entity.TypeInt, IndexType: entity.None},
					{Name: "dept_id", DataType: entity.TypeInt, IndexType: entity.None},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				gotNode, ok := got.(*entity.CreateTableNode)
				if !ok {
					t.Errorf("Parse() got = %T, want *CreateTableNode", got)
					return
				}
				if gotNode.TableName != tt.want.(*entity.CreateTableNode).TableName {
					t.Errorf("TableName = %v, want %v", gotNode.TableName, tt.want.(*entity.CreateTableNode).TableName)
				}
				if !reflect.DeepEqual(gotNode.Columns, tt.want.(*entity.CreateTableNode).Columns) {
					t.Errorf("Columns = %v, want %v", gotNode.Columns, tt.want.(*entity.CreateTableNode).Columns)
				}
			}
		})
	}
}

func TestEdgeCases_InsertAndCreate(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "insert without table name",
			sql:     "INSERT INTO VALUES (1, 'test')",
			wantErr: true,
		},
		{
			name:    "insert without values",
			sql:     "INSERT INTO users (id, name)",
			wantErr: true,
		},
		{
			name:    "create table without columns",
			sql:     "CREATE TABLE users",
			wantErr: true,
		},
		{
			name:    "create table with invalid data type",
			sql:     "CREATE TABLE users (id INVALID_TYPE)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
