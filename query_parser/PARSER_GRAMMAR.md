# MYDB Parser Grammar

The query parser accepts a subset of SQL. This document outlines the supported statements and syntax.

## General
- Keywords are case-insensitive.
- Identifiers are case-sensitive.
- String literals are enclosed in single `'` or double `"` quotes.
- Numeric literals can contain an optional decimal point (`.`).

## Statements

### SELECT
```sql
SELECT select_list FROM table_name
[join_clause]
[WHERE expr]
[GROUP BY ident_list [HAVING expr]]
[ORDER BY order_list]
[LIMIT NUMBER [OFFSET NUMBER]]
;

select_list : '*' | select_item (',' select_item)*
select_item : qualified_ident [AS alias]
            | agg_func '(' [DISTINCT] ('*' | qualified_ident) ')' [AS alias]
agg_func    : COUNT | SUM | AVG | MIN | MAX
join_clause : [INNER | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]] JOIN table_name ON qualified_ident '=' qualified_ident
```

### INSERT
```sql
INSERT INTO table_name ['(' ident_list ')']
VALUES value_list (',' value_list)* ;

value_list : '(' literal_or_null (',' literal_or_null)* ')'
```

### UPDATE
```sql
UPDATE table_name SET assign (',' assign)* [WHERE expr] ;

assign : qualified_ident '=' literal_or_null
```

### DELETE
```sql
DELETE FROM table_name [WHERE expr] ;
```

### CREATE TABLE
```sql
CREATE TABLE table_name '(' table_element (',' table_element)* ')' ;

table_element : column_def | table_constraint
column_def    : IDENT type_spec column_constraint*
type_spec     : INT | INTEGER | DECIMAL ['(' NUMBER ',' NUMBER ')'] | VARCHAR ['(' NUMBER ')'] | ENUM '(' IDENT (',' IDENT)* ')' | BOOL | BOOLEAN | DATE ['(' IDENT ')'] | DATETIME
column_constraint : PRIMARY KEY | NOT NULL | UNIQUE | AUTO_INCREMENT | AUTOINCR | DEFAULT literal_or_null
table_constraint  : [CONSTRAINT IDENT] ( PRIMARY KEY '(' IDENT ')' | FOREIGN KEY '(' IDENT ')' REFERENCES IDENT '(' IDENT ')' )
```

### Other DDL / TCL
```sql
DROP TABLE table_name ;
CREATE DATABASE database_name ;
CREATE SCHEMA schema_name ;
DROP DATABASE database_name ;
DROP SCHEMA schema_name ;
USE schema_name ;
SHOW TABLES ;
SHOW DATABASES ;

BEGIN ;
COMMIT ;
ROLLBACK ;
```

## Expressions

```sql
expr            : or_expr
or_expr         : and_expr (OR and_expr)*
and_expr        : not_expr (AND not_expr)*
not_expr        : [NOT] predicate
predicate       : '(' expr ')'
                | term comparison_op term
                | term IS [NOT] NULL
                | term [NOT] BETWEEN term AND term
                | term [NOT] IN '(' literal_or_null (',' literal_or_null)* ')'
                | term [NOT] LIKE STRING
comparison_op   : '=' | '!=' | '<>' | '<' | '>' | '<=' | '>='
term            : qualified_ident | literal_or_null | '-' NUMBER
qualified_ident : IDENT ['.' IDENT]
literal_or_null : NUMBER | STRING | TRUE | FALSE | NULL
```
