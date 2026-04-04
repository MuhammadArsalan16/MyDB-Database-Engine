#include <iostream>
#include <readline/readline.h>
#include <readline/history.h>
#include "../include/Lexer.hpp"
#include "../include/Parser.hpp"

int main() {
    char* input;
    std::cout << "MyDB Terminal Started. Type 'exit' to quit.\n";

    while ((input = readline("mydb> ")) != nullptr) {
        std::string query(input);
        
        if (query == "exit" || query == ".exit") {
            free(input);
            break;
        }

        if (!query.empty()) {
            add_history(input); // Arrow key history

            try {
                // 1. Lexical Analysis
                Lexer lexer(query);
                std::vector<Token> tokens = lexer.tokenize();

                // 2. Parsing
                Parser parser(tokens);
                std::unique_ptr<ASTNode> ast = parser.parse();

                // 3. Output the AST (This is what you hand to the Query Processor)
                ast->print();

            } catch (const std::exception& e) {
                std::cerr << e.what() << "\n";
            }
        }
        free(input);
    }
    return 0;
}
