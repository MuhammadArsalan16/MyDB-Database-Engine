CC     = gcc
CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -g -D_POSIX_C_SOURCE=200809L \
         -Istorage_engine/include \
         -Isystem_schema/include  \
         -Iengine/include
LDFLAGS = -lcrypto

BUILD = build

# Source files (dependency order)
SRCS = \
    storage_engine/src/disk_manager.c  \
    storage_engine/src/page.c          \
    storage_engine/src/buffer_pool.c   \
    storage_engine/src/checksum.c      \
    storage_engine/src/file_header.c   \
    storage_engine/src/partition.c     \
    storage_engine/src/relation_def.c  \
    storage_engine/src/schema_file.c   \
    storage_engine/src/btree.c         \
    storage_engine/src/transaction.c   \
    system_schema/src/system_schema.c  \
    engine/src/crypto.c                \
    engine/src/database_file.c         \
    engine/src/engine.c                \
    storage_engine/src/storage.c

OBJS = $(patsubst %.c, $(BUILD)/%.o, $(SRCS))

LIB = $(BUILD)/libmydb.a

# Test source directories
TEST_SE_DIR  = tests/storage_engine
TEST_SS_DIR  = tests/system_schema
TEST_ENG_DIR = tests/engine

SE_TESTS = \
    $(BUILD)/test_disk_manager  \
    $(BUILD)/test_page          \
    $(BUILD)/test_buffer_pool   \
    $(BUILD)/test_file_header   \
    $(BUILD)/test_partition     \
    $(BUILD)/test_relation_def  \
    $(BUILD)/test_schema_file   \
    $(BUILD)/test_btree         \
    $(BUILD)/test_transaction

SS_TESTS = $(BUILD)/test_system_schema

ENG_TESTS = \
    $(BUILD)/test_database_file \
    $(BUILD)/test_crypto        \
    $(BUILD)/test_engine        \
    $(BUILD)/test_storage

ALL_TESTS = $(SE_TESTS) $(SS_TESTS) $(ENG_TESTS)

.PHONY: all clean test test_se test_ss test_eng

all: $(LIB)

# Create build subdirectories as needed
$(BUILD)/storage_engine/src $(BUILD)/system_schema/src $(BUILD)/engine/src:
	mkdir -p $@

$(BUILD)/%.o: %.c | $(BUILD)/storage_engine/src $(BUILD)/system_schema/src $(BUILD)/engine/src
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(OBJS)
	ar rcs $@ $^

# storage_engine test binaries
$(BUILD)/test_%: $(TEST_SE_DIR)/test_%.c $(LIB)
	$(CC) $(CFLAGS) $< -L$(BUILD) -lmydb $(LDFLAGS) -o $@

# system_schema test binary
$(BUILD)/test_system_schema: $(TEST_SS_DIR)/test_system_schema.c $(LIB)
	$(CC) $(CFLAGS) $< -L$(BUILD) -lmydb $(LDFLAGS) -o $@

# engine test binaries
$(BUILD)/test_database_file: $(TEST_ENG_DIR)/test_database_file.c $(LIB)
	$(CC) $(CFLAGS) $< -L$(BUILD) -lmydb $(LDFLAGS) -o $@

$(BUILD)/test_crypto: $(TEST_ENG_DIR)/test_crypto.c $(LIB)
	$(CC) $(CFLAGS) $< -L$(BUILD) -lmydb $(LDFLAGS) -o $@

$(BUILD)/test_engine: $(TEST_ENG_DIR)/test_engine.c $(LIB)
	$(CC) $(CFLAGS) $< -L$(BUILD) -lmydb $(LDFLAGS) -o $@

$(BUILD)/test_storage: $(TEST_ENG_DIR)/test_storage.c $(LIB)
	$(CC) $(CFLAGS) $< -L$(BUILD) -lmydb $(LDFLAGS) -o $@

# Run all tests
test: $(ALL_TESTS)
	@echo "--- Running all tests ---"
	@for t in $(ALL_TESTS); do \
		echo ""; \
		echo "Running $$t ..."; \
		$$t; \
	done
	@echo ""
	@echo "--- All tests done ---"

# Run storage_engine tests only
test_se: $(SE_TESTS)
	@echo "--- Running storage_engine tests ---"
	@for t in $(SE_TESTS); do \
		echo ""; \
		echo "Running $$t ..."; \
		$$t; \
	done
	@echo "--- Done ---"

# Run system_schema tests only
test_ss: $(SS_TESTS)
	@echo "--- Running system_schema tests ---"
	@for t in $(SS_TESTS); do \
		echo ""; \
		echo "Running $$t ..."; \
		$$t; \
	done
	@echo "--- Done ---"

# Run engine-level tests only
test_eng: $(ENG_TESTS)
	@echo "--- Running engine tests ---"
	@for t in $(ENG_TESTS); do \
		echo ""; \
		echo "Running $$t ..."; \
		$$t; \
	done
	@echo "--- Done ---"

# Build and run a single test: make run_test_page
run_test_%: $(BUILD)/test_%
	$<

clean:
	rm -rf $(BUILD)
