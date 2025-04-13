# godb
go version database from scratch

tree : b+ tree in memory

database: database api

disktree: b+ tree engine with disk flush
    
    include b+ tree in disk 
    redolog
    lsn
    dirtyPage
    cache
    lru

sqlparser: lexer and parser to sql
