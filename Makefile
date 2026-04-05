CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic -g -D_POSIX_C_SOURCE=200809L -Istorage_engine/include

SRC_DIR = storage_engine/src
INC_DIR = storage_engine/include
TEST_DIR = tests/storage_engine
BUILD   = build

SRCS    = $(SRC_DIR)/disk_manager.c \
          $(SRC_DIR)/page.c         \
          $(SRC_DIR)/buffer_pool.c  \
          $(SRC_DIR)/btree.c        \
          $(SRC_DIR)/schema.c       \
          $(SRC_DIR)/transaction.c  \
          $(SRC_DIR)/storage.c

OBJS    = $(patsubst $(SRC_DIR)/%.c, $(BUILD)/%.o, $(SRCS))

# Static library
LIB     = $(BUILD)/libstorage_engine.a

# Test binaries
TESTS   = $(BUILD)/test_disk_manager \
          $(BUILD)/test_page         \
          $(BUILD)/test_buffer_pool  \
          $(BUILD)/test_btree        \
          $(BUILD)/test_schema       \
          $(BUILD)/test_transaction  \
          $(BUILD)/test_storage

.PHONY: all clean test

all: $(LIB)

$(BUILD):
	mkdir -p $(BUILD)

# Compile each source file to an object
$(BUILD)/%.o: $(SRC_DIR)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

# Build the static library
$(LIB): $(OBJS)
	ar rcs $@ $^

# Build individual test binaries
$(BUILD)/test_%: $(TEST_DIR)/test_%.c $(LIB)
	$(CC) $(CFLAGS) $< -L$(BUILD) -lstorage_engine -o $@

# Run all tests
test: $(TESTS)
	@echo "--- Running tests ---"
	@for t in $(TESTS); do \
		echo ""; \
		echo "Running $$t ..."; \
		$$t; \
	done
	@echo ""
	@echo "--- All tests done ---"

# Build and run a single test: make test_disk_manager
test_%: $(BUILD)/test_%
	$<

clean:
	rm -rf $(BUILD)
