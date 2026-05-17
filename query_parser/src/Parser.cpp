#include "../include/Parser.hpp"
#include <iostream>

Parser::Parser(std::vector<Token> tokens) : tokens(std::move(tokens)) {}

// --- Helper Functions ---

Token Parser::peek() {
    if (current_token_idx >= tokens.size()) return tokens.back();
    return tokens[current_token_idx];
}

Token Parser::advance() {
    if (current_token_idx < tokens.size()) current_token_idx++;
    return tokens[current_token_idx - 1];
}

bool Parser::match(TokenType type, const std::string& value) {
    Token current = peek();
    if (current.type == type && (value.empty() || current.value == value)) {
        advance();
        return true;
    }
    return false;
}

void Parser::throw_error(const std::string& error_msg) {
    Token t = peek();
    std::string got = (t.type == TokenType::END_OF_FILE) ? "EOF" : (t.value.empty() ? "UNKNOWN" : t.value);
    throw std::runtime_error("parse error at " + std::to_string(t.line) + ":" + std::to_string(t.col) + ": " + error_msg + ", got " + got);
}

void Parser::consume(TokenType type, const std::string& value, const std::string& error_msg) {
    if (!match(type, value)) {
        Token t = peek();
        std::string got = (t.type == TokenType::END_OF_FILE) ? "EOF" : (t.value.empty() ? "UNKNOWN" : t.value);
        throw std::runtime_error("parse error at " + std::to_string(t.line) + ":" + std::to_string(t.col) + ": " + error_msg + ", got " + got);
    }
}

std::string Parser::parse_qualified_ident() {
    Token t = advance();
    if (t.type != TokenType::IDENTIFIER && t.type != TokenType::KEYWORD) {
        throw_error("Expected identifier.");
    }
    std::string ident = t.value;
    if (match(TokenType::SYMBOL, ".")) {
        Token t2 = advance();
        if (t2.type != TokenType::IDENTIFIER && t2.type != TokenType::KEYWORD) {
            throw_error("Expected identifier after '.'.");
        }
        ident += "." + t2.value;
    }
    return ident;
}

// --- Main Parsing Dispatcher ---

std::unique_ptr<ASTNode> Parser::parse() {
    if (match(TokenType::KEYWORD, "SELECT")) return parse_select();
    if (match(TokenType::KEYWORD, "CREATE")) {
        if (peek().value == "TABLE") return parse_create_table();
        if (peek().value == "DATABASE" || peek().value == "SCHEMA") return parse_create_database();
        throw_error("Expected TABLE or DATABASE after CREATE");
    }
    if (match(TokenType::KEYWORD, "DROP")) {
        if (peek().value == "TABLE") return parse_drop_table();
        if (peek().value == "DATABASE" || peek().value == "SCHEMA") return parse_drop_database();
        throw_error("Expected TABLE or DATABASE after DROP");
    }
    if (match(TokenType::KEYWORD, "INSERT")) return parse_insert();
    if (match(TokenType::KEYWORD, "UPDATE")) return parse_update();
    if (match(TokenType::KEYWORD, "DELETE")) return parse_delete();
    if (match(TokenType::KEYWORD, "USE")) return parse_use();
    if (match(TokenType::KEYWORD, "SHOW")) return parse_show();
    
    // NEW TCL DISPATCHERS
    if (match(TokenType::KEYWORD, "BEGIN")) return parse_transaction("BEGIN");
    if (match(TokenType::KEYWORD, "COMMIT")) return parse_transaction("COMMIT");
    if (match(TokenType::KEYWORD, "ROLLBACK")) return parse_transaction("ROLLBACK");
    
    throw_error("Unsupported or unrecognized statement.");
}

// --- Statement Parsers ---

// 1. SELECT Statement Logic
std::unique_ptr<SelectStatement> Parser::parse_select() {
    auto stmt = std::make_unique<SelectStatement>();

    // Parse columns
    if (match(TokenType::SYMBOL, "*")) {
        stmt->is_select_all = true;
    } else {
        do {
            Token t = peek();
            if (t.type == TokenType::KEYWORD && (t.value == "COUNT" || t.value == "SUM" || t.value == "AVG" || t.value == "MIN" || t.value == "MAX")) {
                SelectItem item;
                item.kind = SelectItem::Kind::Aggregate;
                item.agg_func = advance().value;
                consume(TokenType::SYMBOL, "(", "Expected ( after aggregate");
                if (match(TokenType::KEYWORD, "DISTINCT")) item.agg_distinct = true;
                if (match(TokenType::SYMBOL, "*")) {
                    item.column = "*";
                } else {
                    item.column = parse_qualified_ident();
                }
                consume(TokenType::SYMBOL, ")", "Expected ) after aggregate");
                if (match(TokenType::KEYWORD, "AS")) {
                    Token alias_tok = advance();
                    if (alias_tok.type != TokenType::IDENTIFIER) throw_error("Expected identifier after AS");
                    item.alias = alias_tok.value;
                }
                stmt->select_list.push_back(item);
            } else {
                SelectItem item;
                item.kind = SelectItem::Kind::Column;
                std::string ident = parse_qualified_ident();
                size_t dot = ident.find('.');
                if (dot != std::string::npos) {
                    item.table = ident.substr(0, dot);
                    item.column = ident.substr(dot + 1);
                } else {
                    item.column = ident;
                }
                if (match(TokenType::KEYWORD, "AS")) {
                    Token alias_tok = advance();
                    if (alias_tok.type != TokenType::IDENTIFIER) throw_error("Expected identifier after AS");
                    item.alias = alias_tok.value;
                }
                stmt->select_list.push_back(item);
            }
        } while (match(TokenType::SYMBOL, ",")); 
    }

    consume(TokenType::KEYWORD, "FROM", "Expected 'FROM' after column list.");

    // Parse Table Name
    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw_error("Expected table name.");
    stmt->table_name = table_token.value;

    // --- NEW: Parse Optional JOIN Clause ---
// --- FULL JOIN PARSING LOGIC ---
    bool has_join = false;
    JoinType j_type = JoinType::INNER;

    if (match(TokenType::KEYWORD, "LEFT")) {
        match(TokenType::KEYWORD, "OUTER"); // 'OUTER' is optional in SQL, so we just match and ignore
        consume(TokenType::KEYWORD, "JOIN", "Expected 'JOIN' after 'LEFT'.");
        j_type = JoinType::LEFT;
        has_join = true;
    } else if (match(TokenType::KEYWORD, "RIGHT")) {
        match(TokenType::KEYWORD, "OUTER");
        consume(TokenType::KEYWORD, "JOIN", "Expected 'JOIN' after 'RIGHT'.");
        j_type = JoinType::RIGHT;
        has_join = true;
    } else if (match(TokenType::KEYWORD, "FULL")) {
        match(TokenType::KEYWORD, "OUTER");
        consume(TokenType::KEYWORD, "JOIN", "Expected 'JOIN' after 'FULL'.");
        j_type = JoinType::FULL;
        has_join = true;
    } else if (match(TokenType::KEYWORD, "INNER")) {
        consume(TokenType::KEYWORD, "JOIN", "Expected 'JOIN' after 'INNER'.");
        j_type = JoinType::INNER;
        has_join = true;
    } else if (match(TokenType::KEYWORD, "JOIN")) {
        j_type = JoinType::INNER; // Standard JOIN is an INNER JOIN
        has_join = true;
    }

    if (has_join) {
        stmt->join_clause = std::make_unique<JoinClause>();
        stmt->join_clause->join_type = j_type;
        
        Token join_table_token = advance();
        if (join_table_token.type != TokenType::IDENTIFIER) throw_error("Expected table name after JOIN.");
        stmt->join_clause->join_table = join_table_token.value;
consume(TokenType::KEYWORD, "ON", "Expected 'ON' after JOIN table name.");
        
        std::string left_cond = parse_qualified_ident();
        if (left_cond.find('.') == std::string::npos) {
            throw_error("Left side of ON condition must be in 'table.column' format (e.g., users.id).");
        }
        stmt->join_clause->left_condition = left_cond;

        consume(TokenType::OPERATOR, "=", "Expected '=' in JOIN condition.");
        
        std::string right_cond = parse_qualified_ident();
        if (right_cond.find('.') == std::string::npos) {
            throw_error("Right side of ON condition must be in 'table.column' format (e.g., orders.user_id).");
        }
        stmt->join_clause->right_condition = right_cond;
    }

    // Parse Optional WHERE Clause
    if (match(TokenType::KEYWORD, "WHERE")) {
        stmt->where_clause = std::make_unique<WhereClause>();
        stmt->where_clause->root = parse_expr();
    }

    if (match(TokenType::KEYWORD, "GROUP")) {
        consume(TokenType::KEYWORD, "BY", "Expected BY after GROUP");
        do {
            stmt->group_by.push_back(parse_qualified_ident());
        } while (match(TokenType::SYMBOL, ","));
        if (match(TokenType::KEYWORD, "HAVING")) {
            stmt->having = parse_expr();
        }
    }

    if (match(TokenType::KEYWORD, "ORDER")) {
        consume(TokenType::KEYWORD, "BY", "Expected BY after ORDER");
        do {
            OrderByItem item;
            std::string ident = parse_qualified_ident();
            size_t dot = ident.find('.');
            if (dot != std::string::npos) {
                item.table = ident.substr(0, dot);
                item.column = ident.substr(dot + 1);
            } else {
                item.column = ident;
            }
            if (match(TokenType::KEYWORD, "DESC")) {
                item.descending = true;
            } else {
                match(TokenType::KEYWORD, "ASC");
            }
            stmt->order_by.push_back(item);
        } while (match(TokenType::SYMBOL, ","));
    }

    if (match(TokenType::KEYWORD, "LIMIT")) {
        Token n = advance();
        if (n.type != TokenType::NUMBER) throw_error("Expected number after LIMIT");
        stmt->limit = std::stoi(n.value);
        if (match(TokenType::KEYWORD, "OFFSET")) {
            Token o = advance();
            if (o.type != TokenType::NUMBER) throw_error("Expected number after OFFSET");
            stmt->offset = std::stoi(o.value);
        }
    }

    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");

    return stmt;
}


// 2. CREATE TABLE Statement Logic
std::unique_ptr<CreateTableStatement> Parser::parse_create_table() {
    auto stmt = std::make_unique<CreateTableStatement>();

    consume(TokenType::KEYWORD, "TABLE", "Expected 'TABLE' after 'CREATE'.");
    
    // Parse Table Name
    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw_error("Expected table name.");
    stmt->table_name = table_token.value;

    consume(TokenType::SYMBOL, "(", "Expected '(' to define columns.");

    // Parse elements loop
    do {
        bool is_constraint = false;
        Token t = peek();
        if (t.type == TokenType::KEYWORD && (t.value == "CONSTRAINT" || t.value == "PRIMARY" || t.value == "FOREIGN")) {
            is_constraint = true;
        }

        if (is_constraint) {
            std::string constraint_name;
            if (match(TokenType::KEYWORD, "CONSTRAINT")) {
                Token name_tok = peek();
                if (name_tok.type == TokenType::IDENTIFIER) {
                    constraint_name = advance().value;
                }
            }
            if (match(TokenType::KEYWORD, "PRIMARY")) {
                consume(TokenType::KEYWORD, "KEY", "Expected KEY after PRIMARY");
                consume(TokenType::SYMBOL, "(", "Expected (");
                std::string col_name = parse_qualified_ident();
                consume(TokenType::SYMBOL, ")", "Expected )");
                
                bool found = false;
                for (auto& c : stmt->columns) {
                    if (c.name == col_name) { c.is_primary_key = true; found = true; break; }
                }
                if (!found) throw_error("PRIMARY KEY column not found.");
            } else if (match(TokenType::KEYWORD, "FOREIGN")) {
                consume(TokenType::KEYWORD, "KEY", "Expected KEY after FOREIGN");
                consume(TokenType::SYMBOL, "(", "Expected (");
                std::string col_name = parse_qualified_ident();
                consume(TokenType::SYMBOL, ")", "Expected )");
                consume(TokenType::KEYWORD, "REFERENCES", "Expected REFERENCES");
                std::string ref_table = advance().value;
                consume(TokenType::SYMBOL, "(", "Expected (");
                std::string ref_col = parse_qualified_ident();
                consume(TokenType::SYMBOL, ")", "Expected )");
                
                AstForeignKey fk;
                fk.constraint_name = constraint_name;
                fk.column_name = col_name;
                fk.ref_table = ref_table;
                fk.ref_column = ref_col;
                stmt->foreign_keys.push_back(fk);
            } else {
                throw_error("Unknown constraint type.");
            }
        } else {
            ASTColumnDef col;
            Token name_token = advance();
            if (name_token.type != TokenType::IDENTIFIER) throw_error("Expected column name.");
            col.name = name_token.value;

            Token type_token = advance();
            if (type_token.type != TokenType::IDENTIFIER && type_token.type != TokenType::KEYWORD) {
                throw_error("Expected data type.");
            }
            std::string type_str = type_token.value;
            col.data_type = type_str;

            // qualifiers
            if (type_str == "DECIMAL") {
                if (match(TokenType::SYMBOL, "(")) {
                    col.max_len = std::stoi(advance().value);
                    consume(TokenType::SYMBOL, ",", "Expected , in DECIMAL");
                    col.scale = std::stoi(advance().value);
                    consume(TokenType::SYMBOL, ")", "Expected )");
                }
            } else if (type_str == "VARCHAR") {
                if (match(TokenType::SYMBOL, "(")) {
                    col.max_len = std::stoi(advance().value);
                    consume(TokenType::SYMBOL, ")", "Expected )");
                }
            } else if (type_str == "ENUM") {
                consume(TokenType::SYMBOL, "(", "Expected ( in ENUM");
                do {
                    col.enum_values.push_back(advance().value);
                } while (match(TokenType::SYMBOL, ","));
                consume(TokenType::SYMBOL, ")", "Expected )");
            } else if (type_str == "DATE") {
                if (match(TokenType::SYMBOL, "(")) {
                    col.date_format = advance().value;
                    consume(TokenType::SYMBOL, ")", "Expected )");
                }
            }

            // Constraints loop
            while (true) {
                if (match(TokenType::KEYWORD, "PRIMARY")) {
                    consume(TokenType::KEYWORD, "KEY", "Expected KEY");
                    col.is_primary_key = true;
                } else if (match(TokenType::KEYWORD, "NOT")) {
                    consume(TokenType::KEYWORD, "NULL", "Expected NULL");
                    col.is_not_null = true;
                } else if (match(TokenType::KEYWORD, "UNIQUE")) {
                    col.is_unique = true;
                } else if (match(TokenType::KEYWORD, "AUTO_INCREMENT") || match(TokenType::KEYWORD, "AUTOINCR")) {
                    col.is_auto_increment = true;
                } else if (match(TokenType::KEYWORD, "DEFAULT")) {
                    col.has_default = true;
                    Token def_tok = advance();
                    col.default_kind = static_cast<int>(def_tok.type);
                    col.default_text = def_tok.value;
                } else {
                    break;
                }
            }

            stmt->columns.push_back(col);
        }
    } while (match(TokenType::SYMBOL, ","));

    consume(TokenType::SYMBOL, ")", "Expected ')' to close column definitions.");
    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");

    return stmt;
}

// 3. INSERT INTO Statement Logic
std::unique_ptr<InsertStatement> Parser::parse_insert() {
    auto stmt = std::make_unique<InsertStatement>();

    consume(TokenType::KEYWORD, "INTO", "Expected 'INTO' after 'INSERT'.");
    
    // Parse Table Name
    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw_error("Expected table name.");
    stmt->table_name = table_token.value;

    // Optional columns
    if (match(TokenType::SYMBOL, "(")) {
        do {
            stmt->target_columns.push_back(parse_qualified_ident());
        } while (match(TokenType::SYMBOL, ","));
        consume(TokenType::SYMBOL, ")", "Expected ')' after column list.");
    }

    consume(TokenType::KEYWORD, "VALUES", "Expected 'VALUES' keyword.");
    consume(TokenType::SYMBOL, "(", "Expected '(' before values.");

    // Parse values loop
    std::vector<std::string> row;
    do {
        Token val_token = advance();
        if (val_token.type != TokenType::NUMBER && val_token.type != TokenType::STRING && val_token.type != TokenType::KEYWORD) {
            throw_error("Invalid literal in INSERT.");
        }
        if (val_token.type == TokenType::KEYWORD && val_token.value != "TRUE" && val_token.value != "FALSE" && val_token.value != "NULL") {
            throw_error("Invalid literal in INSERT.");
        }
        row.push_back(val_token.value);
    } while (match(TokenType::SYMBOL, ","));
    stmt->rows.push_back(row);

    consume(TokenType::SYMBOL, ")", "Expected ')' after values.");
    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");

    return stmt;
}




// Logic for UPDATE statements
std::unique_ptr<UpdateStatement> Parser::parse_update() {
    auto stmt = std::make_unique<UpdateStatement>();

    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw_error("Expected table name after UPDATE.");
    stmt->table_name = table_token.value;

    consume(TokenType::KEYWORD, "SET", "Expected 'SET' keyword.");

    do {
        std::string col = parse_qualified_ident();
        consume(TokenType::OPERATOR, "=", "Expected '=' after column name in SET clause.");
        Token val_token = advance();
        if (val_token.type != TokenType::NUMBER && val_token.type != TokenType::STRING && val_token.type != TokenType::KEYWORD) {
            throw_error("Invalid literal in UPDATE.");
        }
        if (val_token.type == TokenType::KEYWORD && val_token.value != "TRUE" && val_token.value != "FALSE" && val_token.value != "NULL") {
            throw_error("Invalid literal in UPDATE.");
        }
        stmt->assignments.push_back({col, val_token.value});
    } while (match(TokenType::SYMBOL, ","));

    // Optional WHERE clause
    if (match(TokenType::KEYWORD, "WHERE")) {
        stmt->where_clause = std::make_unique<WhereClause>();
        stmt->where_clause->root = parse_expr();
    }

    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");
    return stmt;
}

// Logic for DELETE statements
std::unique_ptr<DeleteStatement> Parser::parse_delete() {
    auto stmt = std::make_unique<DeleteStatement>();

    consume(TokenType::KEYWORD, "FROM", "Expected 'FROM' after DELETE.");

    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw_error("Expected table name.");
    stmt->table_name = table_token.value;

    // Optional WHERE clause
    if (match(TokenType::KEYWORD, "WHERE")) {
        stmt->where_clause = std::make_unique<WhereClause>();
        stmt->where_clause->root = parse_expr();
    }

    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");
    return stmt;
}


// Logic for TCL (Transactions)
std::unique_ptr<TransactionStatement> Parser::parse_transaction(const std::string& cmd_str) {
    TransactionCommand cmd;
    
    if (cmd_str == "BEGIN") cmd = TransactionCommand::BEGIN;
    else if (cmd_str == "COMMIT") cmd = TransactionCommand::COMMIT;
    else if (cmd_str == "ROLLBACK") cmd = TransactionCommand::ROLLBACK;

    consume(TokenType::SYMBOL, ";", "Expected ';' after transaction command.");
    
    return std::make_unique<TransactionStatement>(cmd);
}

// --- Expression Parsing ---
std::unique_ptr<Expr> Parser::parse_expr() {
    return parse_or_expr();
}

std::unique_ptr<Expr> Parser::parse_or_expr() {
    auto node = parse_and_expr();
    while (match(TokenType::KEYWORD, "OR")) {
        auto bin = std::make_unique<BinaryExpr>();
        bin->op = "OR";
        bin->lhs = std::move(node);
        bin->rhs = parse_and_expr();
        node = std::move(bin);
    }
    return node;
}

std::unique_ptr<Expr> Parser::parse_and_expr() {
    auto node = parse_not_expr();
    while (match(TokenType::KEYWORD, "AND")) {
        auto bin = std::make_unique<BinaryExpr>();
        bin->op = "AND";
        bin->lhs = std::move(node);
        bin->rhs = parse_not_expr();
        node = std::move(bin);
    }
    return node;
}

std::unique_ptr<Expr> Parser::parse_not_expr() {
    if (match(TokenType::KEYWORD, "NOT")) {
        auto un = std::make_unique<UnaryExpr>();
        un->op = "NOT";
        un->child = parse_predicate();
        return un;
    }
    return parse_predicate();
}

std::unique_ptr<Expr> Parser::parse_predicate() {
    if (match(TokenType::SYMBOL, "(")) {
        auto node = parse_expr();
        consume(TokenType::SYMBOL, ")", "Expected ')' after expression");
        return node;
    }

    auto term = parse_term();

    // Check operators
    Token t = peek();
    if (t.type == TokenType::OPERATOR || t.type == TokenType::SYMBOL) {
        if (t.type == TokenType::OPERATOR || (t.type == TokenType::SYMBOL && t.value == "=")) {
            advance();
            auto bin = std::make_unique<BinaryExpr>();
            bin->op = t.value;
            bin->lhs = std::move(term);
            bin->rhs = parse_term();
            return bin;
        }
    }
    
    if (match(TokenType::KEYWORD, "IS")) {
        bool negated = match(TokenType::KEYWORD, "NOT");
        consume(TokenType::KEYWORD, "NULL", "Expected NULL after IS [NOT]");
        auto isn = std::make_unique<IsNullExpr>();
        isn->child = std::move(term);
        isn->negated = negated;
        return isn;
    } else if (match(TokenType::KEYWORD, "BETWEEN")) {
        auto bet = std::make_unique<BetweenExpr>();
        bet->v = std::move(term);
        bet->negated = false;
        bet->lo = parse_term();
        consume(TokenType::KEYWORD, "AND", "Expected AND in BETWEEN clause");
        bet->hi = parse_term();
        return bet;
    } else if (match(TokenType::KEYWORD, "NOT")) {
        if (match(TokenType::KEYWORD, "BETWEEN")) {
            auto bet = std::make_unique<BetweenExpr>();
            bet->v = std::move(term);
            bet->negated = true;
            bet->lo = parse_term();
            consume(TokenType::KEYWORD, "AND", "Expected AND in BETWEEN clause");
            bet->hi = parse_term();
            return bet;
        } else if (match(TokenType::KEYWORD, "IN")) {
            auto inx = std::make_unique<InExpr>();
            inx->v = std::move(term);
            inx->negated = true;
            consume(TokenType::SYMBOL, "(", "Expected ( after IN");
            do {
                Token vt = advance();
                if (vt.type != TokenType::NUMBER && vt.type != TokenType::STRING && vt.type != TokenType::KEYWORD) {
                    throw_error("Expected literal in IN list");
                }
                auto lit = std::make_unique<LiteralExpr>();
                lit->type = vt.type;
                lit->raw = vt.value;
                inx->list.push_back(std::move(lit));
            } while (match(TokenType::SYMBOL, ","));
            consume(TokenType::SYMBOL, ")", "Expected ) after IN list");
            return inx;
        } else if (match(TokenType::KEYWORD, "LIKE")) {
            auto lk = std::make_unique<LikeExpr>();
            lk->v = std::move(term);
            lk->negated = true;
            Token pat = advance();
            if (pat.type != TokenType::STRING) throw_error("Expected string after LIKE");
            lk->pattern = pat.value;
            return lk;
        }
        throw_error("Unexpected NOT");
    } else if (match(TokenType::KEYWORD, "IN")) {
        auto inx = std::make_unique<InExpr>();
        inx->v = std::move(term);
        inx->negated = false;
        consume(TokenType::SYMBOL, "(", "Expected ( after IN");
        do {
            Token vt = advance();
            if (vt.type != TokenType::NUMBER && vt.type != TokenType::STRING && vt.type != TokenType::KEYWORD) {
                throw_error("Expected literal in IN list");
            }
            auto lit = std::make_unique<LiteralExpr>();
            lit->type = vt.type;
            lit->raw = vt.value;
            inx->list.push_back(std::move(lit));
        } while (match(TokenType::SYMBOL, ","));
        consume(TokenType::SYMBOL, ")", "Expected ) after IN list");
        return inx;
    } else if (match(TokenType::KEYWORD, "LIKE")) {
        auto lk = std::make_unique<LikeExpr>();
        lk->v = std::move(term);
        lk->negated = false;
        Token pat = advance();
        if (pat.type != TokenType::STRING) throw_error("Expected string after LIKE");
        lk->pattern = pat.value;
        return lk;
    }

    return term;
}

std::unique_ptr<Expr> Parser::parse_term() {
    Token t = peek();
    if (t.type == TokenType::NUMBER || t.type == TokenType::STRING || 
        (t.type == TokenType::KEYWORD && (t.value == "TRUE" || t.value == "FALSE" || t.value == "NULL"))) {
        advance();
        auto lit = std::make_unique<LiteralExpr>();
        lit->type = t.type;
        lit->raw = t.value;
        return lit;
    }
    
    // Unary minus for numbers
    if (t.type == TokenType::OPERATOR && t.value == "-") {
        advance();
        Token nt = advance();
        if (nt.type != TokenType::NUMBER) throw_error("Expected number after -");
        auto lit = std::make_unique<LiteralExpr>();
        lit->type = nt.type;
        lit->raw = "-" + nt.value;
        return lit;
    }

    auto col = std::make_unique<ColumnRefExpr>();
    std::string ident = parse_qualified_ident();
    size_t dot = ident.find('.');
    if (dot != std::string::npos) {
        col->table = ident.substr(0, dot);
        col->column = ident.substr(dot + 1);
    } else {
        col->column = ident;
    }
    return col;
}

// --- DDL and USE Parsing ---
std::unique_ptr<DropTableStatement> Parser::parse_drop_table() {
    auto stmt = std::make_unique<DropTableStatement>();
    consume(TokenType::KEYWORD, "TABLE", "Expected TABLE after DROP");
    Token t = advance();
    if (t.type != TokenType::IDENTIFIER) throw_error("Expected table name");
    stmt->table_name = t.value;
    consume(TokenType::SYMBOL, ";", "Expected ;");
    return stmt;
}

std::unique_ptr<CreateDatabaseStatement> Parser::parse_create_database() {
    auto stmt = std::make_unique<CreateDatabaseStatement>();
    Token t1 = advance(); // DATABASE or SCHEMA
    Token t = advance();
    if (t.type != TokenType::IDENTIFIER) throw_error("Expected database name");
    stmt->name = t.value;
    consume(TokenType::SYMBOL, ";", "Expected ;");
    return stmt;
}

std::unique_ptr<DropDatabaseStatement> Parser::parse_drop_database() {
    auto stmt = std::make_unique<DropDatabaseStatement>();
    Token t1 = advance(); // DATABASE or SCHEMA
    Token t = advance();
    if (t.type != TokenType::IDENTIFIER) throw_error("Expected database name");
    stmt->name = t.value;
    consume(TokenType::SYMBOL, ";", "Expected ;");
    return stmt;
}

std::unique_ptr<UseStatement> Parser::parse_use() {
    auto stmt = std::make_unique<UseStatement>();
    Token t = advance();
    if (t.type != TokenType::IDENTIFIER) throw_error("Expected schema name after USE");
    stmt->schema_name = t.value;
    consume(TokenType::SYMBOL, ";", "Expected ;");
    return stmt;
}

std::unique_ptr<ASTNode> Parser::parse_show() {
    Token t = advance();
    if (t.type != TokenType::KEYWORD) throw_error("Expected TABLES or DATABASES after SHOW");
    std::unique_ptr<ASTNode> stmt;
    if (t.value == "TABLES") {
        stmt = std::make_unique<ShowTablesStatement>();
    } else if (t.value == "DATABASES") {
        stmt = std::make_unique<ShowDatabasesStatement>();
    } else {
        throw_error("Expected TABLES or DATABASES after SHOW");
    }
    consume(TokenType::SYMBOL, ";", "Expected ;");
    return stmt;
}