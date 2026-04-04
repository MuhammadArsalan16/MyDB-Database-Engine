#include <iostream>
#include <string>
#include <readline/readline.h>
#include <readline/history.h>
#include "../include/Lexer.hpp"
#include "../include/Parser.hpp"

int main() {
    char* input;
    std::string query_buffer = ""; // This holds our multi-line query

    std::cout << "MyDB Terminal Started. Type 'exit' to quit.\n";

    // If buffer is empty, show "mydb> ". If we are in the middle of a query, show "  ... "
    while ((input = readline(query_buffer.empty() ? "mydb> " : "  ... ")) != nullptr) {
        std::string line(input);
        
        // Handle immediate exit
        if (line == "exit" || line == ".exit") {
            free(input);
            break;
        }

        if (!line.empty()) {
            add_history(input); // Add to arrow-key history
            
            // Append the new line to our buffer with a space
            query_buffer += line + " "; 

            // Check if the query is finished (contains a semicolon)
            if (query_buffer.find(';') != std::string::npos) {
                try {
                    Lexer lexer(query_buffer);
                    std::vector<Token> tokens = lexer.tokenize();

                    Parser parser(tokens);
                    std::unique_ptr<ASTNode> ast = parser.parse();

                    ast->print(); // Or pass to QueryProcessor!

                } catch (const std::exception& e) {
                    std::cerr << e.what() << "\n";
                }
                
                // CRITICAL: Clear the buffer so we can start a new query!
                query_buffer.clear();
            }
        }
        free(input);
    }
    return 0;
}