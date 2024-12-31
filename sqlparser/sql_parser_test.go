package lpjsonparser

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// @Title        sql_parser_test.go
// @Description
// @Create       david 2024-12-27 18:13
// @Update       david 2024-12-27 18:13
// 辅助函数：打印 AST 节点的详细信息
func diffNode(got, want ASTNode, path string) string {
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
	case *SelectNode:
		g, ok := got.(*SelectNode)
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

	case *ColumnNode:
		g, ok := got.(*ColumnNode)
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

	case *BinaryOpNode:
		g, ok := got.(*BinaryOpNode)
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

	case *JoinNode:
		g, ok := got.(*JoinNode)
		if !ok {
			return fmt.Sprintf("%s: type mismatch: got %T, want JoinNode", path, got)
		}
		if g.TableName != w.TableName {
			diffs = append(diffs, fmt.Sprintf("%s.TableName: got %q, want %q", path, g.TableName, w.TableName))
		}
		if d := diffNode(g.Condition, w.Condition, path+".Condition"); d != "" {
			diffs = append(diffs, d)
		}

	case *LiteralNode:
		g, ok := got.(*LiteralNode)
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
		want    ASTNode
		wantErr bool
	}{
		{
			name: "simple select",
			sql:  "SELECT id, name FROM users",
			want: &SelectNode{
				TableName: "users",
				Columns: []*ColumnNode{
					newColumnNode("", "id", PLAIN_STRING),
					newColumnNode("", "name", PLAIN_STRING),
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
			want: &SelectNode{
				TableName: "users",
				Columns: []*ColumnNode{
					newColumnNode("", "id", PLAIN_STRING),
				},
				WhereClause: []*BinaryOpNode{
					newBinaryOpNode("=",
						newColumnNode("", "id", PLAIN_STRING),
						newLiteralNode(1),
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
			want: &SelectNode{
				TableName: "users",
				Columns: []*ColumnNode{
					newColumnNode("users", "id", TABLE_NAME_PREFIXED),
					newColumnNode("departments", "name", TABLE_NAME_PREFIXED),
				},
				Join: []*JoinNode{
					newJoinNode("departments",
						newBinaryOpNode("=",
							newColumnNode("users", "dept_id", TABLE_NAME_PREFIXED),
							newColumnNode("departments", "id", TABLE_NAME_PREFIXED),
						),
					),
				},
			},
			wantErr: false,
		},
		{
			name: "select with order by",
			sql:  "SELECT id, name FROM users ORDER BY name",
			want: &SelectNode{
				TableName: "users",
				Columns: []*ColumnNode{
					newColumnNode("", "id", PLAIN_STRING),
					newColumnNode("", "name", PLAIN_STRING),
				},
				OrderByColumns: []*ColumnNode{
					newColumnNode("", "name", PLAIN_STRING),
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
		left := newColumnNode("", "id", PLAIN_STRING)
		right := newLiteralNode(1)
		node := newBinaryOpNode("=", left, right)

		if node.Operator != "=" {
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
		node := newColumnNode("users", "id", TABLE_NAME_PREFIXED)

		if node.TableName != "users" {
			t.Errorf("Expected table name 'users', got %s", node.TableName)
		}
		if node.ColumnName != "id" {
			t.Errorf("Expected column name 'id', got %s", node.ColumnName)
		}
		if node.ColumnType != TABLE_NAME_PREFIXED {
			t.Errorf("Expected column type TABLE_NAME_PREFIXED, got %v", node.ColumnType)
		}
	})

	t.Run("test select node", func(t *testing.T) {
		columns := []*ColumnNode{newColumnNode("", "id", PLAIN_STRING)}
		whereClause := []*BinaryOpNode{
			newBinaryOpNode("=",
				newColumnNode("", "id", PLAIN_STRING),
				newLiteralNode(1),
			),
		}
		node := newSelectNode("users", columns, whereClause, nil, nil)

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
	columns := []*ColumnDefinition{
		{Name: "id", DataType: INT, PrimaryKey: true},
		{Name: "name", DataType: CHAR, PrimaryKey: false},
	}

	node := newCreateTbaleNode("users", columns)

	if node.TableName != "users" {
		t.Errorf("Expected table name 'users', got %s", node.TableName)
	}

	if len(node.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(node.Columns))
	}

	if !node.Columns[0].PrimaryKey {
		t.Error("Expected first column to be primary key")
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
		//{
		//	name:    "select without columns",
		//	sql:     "SELECT FROM users",
		//	wantErr: true,
		//},
		//{
		//	name:    "select without table",
		//	sql:     "SELECT id",
		//	wantErr: true,
		//},
		//{
		//	name:    "incomplete where clause",
		//	sql:     "SELECT id FROM users WHERE",
		//	wantErr: true,
		//},
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
