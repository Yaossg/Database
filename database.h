#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <stdnoreturn.h>

#define KEY_LEN 31U
#define VALUE_LEN 255U

typedef char KEY_TYPE[KEY_LEN + 1];
typedef char VALUE_TYPE[VALUE_LEN + 1];

typedef struct Record {
    KEY_TYPE key;
    VALUE_TYPE value;
} Record;

typedef struct Branch {
    uint32_t child; // left child
    KEY_TYPE key;
} Branch;


#define PAGE_SIZE 4096U

#define DB_TEST_SIZE // make node size small for test
#ifndef DB_TEST_SIZE
#define INTERNAL_BRANCH_SIZE ((PAGE_SIZE - 8 - 4) / (sizeof(Branch))) // 113
#define INTERNAL_LEFT_SPLIT_SIZE (INTERNAL_BRANCH_SIZE / 2) // 56
#define INTERNAL_RIGHT_SPLIT_SIZE (INTERNAL_BRANCH_SIZE - INTERNAL_LEFT_SPLIT_SIZE) // 57
#define LEAF_RECORD_SIZE ((PAGE_SIZE - 8) / sizeof(Record)) // 14
#define LEAF_LEFT_SPLIT_SIZE (LEAF_RECORD_SIZE / 2) // 7
#define LEAF_RIGHT_SPLIT_SIZE (LEAF_RECORD_SIZE - LEAF_LEFT_SPLIT_SIZE) // 7
#else
#define INTERNAL_BRANCH_SIZE 4
#define LEAF_LEFT_SPLIT_SIZE 2
#define LEAF_RIGHT_SPLIT_SIZE 2
#define LEAF_RECORD_SIZE 4
#define INTERNAL_LEFT_SPLIT_SIZE 2
#define INTERNAL_RIGHT_SPLIT_SIZE 2
#endif

enum { NODE_INTERNAL, NODE_LEAF };

typedef struct Page {
    uint8_t type;
    uint8_t is_root;
    uint16_t size;
    uint32_t parent;
    union {
        struct {
            Branch branches[INTERNAL_BRANCH_SIZE];
            uint32_t rightmost; // rightmost child stored individually
        } internal;
        struct {
            Record records[LEAF_RECORD_SIZE];
        } leaf;
    };
} Page;


#define TABLE_MAX_PAGES 100
#define ROOT_PAGE_INDEX 0

typedef struct Table {
    pthread_mutex_t mtx;
    int file;
    uint32_t size;
    Page* pages[TABLE_MAX_PAGES];
} Table;


Table* db_open(const char* filename);
void db_close(Table* table);

void db_set(Table* table, const char* key, const char* value);
const char* db_get(Table* table, const char* key);

void db_test_print(Table* table);
