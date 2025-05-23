# godb
go version database from scratch

## project structure:
```
.
├── LICENSE
├── README.md
├── database (abstract as DB)
│   ├── data
│   ├── database.go
│   ├── database_test.go
│   ├── encoding.go
│   ├── executeResult.go
│   ├── sqlQueryExecutor.go
│   └── sqlTableManager.go
├── disktree (B+ tree main structure save on disk)
│   ├── LRU.go
│   ├── diskpager.go
│   ├── diskpager_test.go
│   ├── file.go
│   ├── hint.md
│   ├── hintForFile.md
│   ├── internal_node.go
│   ├── leaf_node.go
│   ├── node.go
│   ├── redoLog.go
│   ├── test.log
│   ├── test_disk.db
│   ├── test_redolog.log
│   ├── tree.go
│   └── tree_test.go
├── entity (Types)
│   ├── ASTNode.go
│   ├── ColumnDefinition.go
│   ├── DataType.go
│   ├── Token.go
│   └── sqlTableDefinition.go
├── logger
│   └── logger.go
├── main.go
├── sqlparser (sqlParser)
│   ├── lexer.go
│   ├── lexer_test.go
│   ├── parser.go
│   ├── parser_test.go
│   └── test.go
├── transaction
├── tree (simple B+ tree on memory)
│   ├── entry.go
│   ├── internal_node.go
│   ├── leaf_node.go
│   ├── node.go
│   └── tree.go
├── go.mod
├── himt.md
├── hint.txt
├── image.png
└── users.db
```

## SQL support:
CREATE
INSERT
UPDATE
SELECT (support where order)

## index support:
primary key
secondary keys

## data type support:
INT
CHAR

## DISKTREE: b+ tree engine with disk flush

    include b+ tree in disk 
    redolog
    lsn
    dirtyPage
    cache
    lru

## SQLPARSER: lexer and parser to sql