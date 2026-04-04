#!/bin/bash
echo "Compiling MyDB..."
g++ -std=c++20 src/main.cpp src/Lexer.cpp src/Parser.cpp -o mydb -lreadline
echo "Compilation complete. Run using ./mydb"
