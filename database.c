#include "database.h"

// helpers and utilities

noreturn void unreachable() {
    fprintf(stderr, "reach unreachable");
    __builtin_unreachable();
}

void validate_string(const char* string, const char* name, uint32_t max_len) {
    if (string == NULL) {
        fprintf(stderr, "%s must not be NULL", name);
        exit(EXIT_FAILURE);
    }
    size_t len = strlen(string);
    if (len > max_len) {
        fprintf(stderr, "%s(%s) is too long. %zu > %zu", name, string, len, KEY_LEN);
        exit(EXIT_FAILURE);
    }
}

void validate_key(const char* key) {
    validate_string(key, "Key", KEY_LEN);
}

void validate_value(const char* value) {
    validate_string(value, "Value", VALUE_LEN);
}

void initialize_leaf_node(Page* page) {
    page->type = NODE_LEAF;
    page->is_root = false;
    page->size = 0;
}

void initialize_internal_node(Page* page) {
    page->type = NODE_INTERNAL;
    page->is_root = false;
    page->size = 0;
}

char* internal_key_of(Page* page, uint32_t index) {
    return page->internal.branches[index].key;
}

uint32_t* internal_rightmost_child_of(Page* page) {
    return &page->internal.rightmost;
}

uint32_t* internal_child_of(Page* page, uint32_t index) {
    if (index >= page->size) return internal_rightmost_child_of(page);
    return &page->internal.branches[index].child;
}

Page* get_page(Table* table, uint32_t page_index);

const char* recursive_max_key_of(Table* table, Page* page) {
    switch (page->type) {
        case NODE_LEAF:
            return page->leaf.records[page->size - 1].key;
        case NODE_INTERNAL:
            return recursive_max_key_of(table, get_page(table, *internal_rightmost_child_of(page)));
    }

    unreachable();
}

// persistence

Page* get_page(Table* table, uint32_t page_index) {
    if (page_index >= TABLE_MAX_PAGES) {
        fprintf(stderr, "Page number out of bounds. %d >= %d\n", page_index, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (table->pages[page_index] == NULL) {

        Page* page = malloc(PAGE_SIZE);
        if (page_index < table->size) {
            lseek(table->file, page_index * PAGE_SIZE, SEEK_SET);
            if (read(table->file, page, PAGE_SIZE) == -1) {
                fprintf(stderr, "Error during reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        table->pages[page_index] = page;
        if (page_index >= table->size) {
            table->size = page_index + 1;
        }

    }

    return table->pages[page_index];
}

Table* db_open(const char* filename) {
    Table* table = malloc(sizeof(Table));

    pthread_mutex_init(&table->mtx, NULL);

    table->file = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (table->file == -1) {
        fprintf(stderr, "Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t length = lseek(table->file, 0, SEEK_END);
    table->size = length / PAGE_SIZE;
    if (length % PAGE_SIZE != 0) {
        fprintf(stderr, "Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        table->pages[i] = NULL;
    }

    if (table->size == 0) {
        Page* page = get_page(table, ROOT_PAGE_INDEX);
        initialize_leaf_node(page);
        page->is_root = true;
    }

    return table;
}

void flush_page(Table* table, uint32_t page_index) {
    off_t offset = lseek(table->file, page_index * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        fprintf(stderr, "Error during seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    if (write(table->file, table->pages[page_index], PAGE_SIZE) == -1) {
        fprintf(stderr, "Error during writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table* table) {
    for (uint32_t i = 0; i < table->size; i++) {
        if (table->pages[i] != NULL) {
            flush_page(table, i);
            free(table->pages[i]);
            table->pages[i] = NULL;
        }
    }

    if (close(table->file) == -1) {
        fprintf(stderr, "Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    pthread_mutex_destroy(&table->mtx);

    free(table);
}

// searching

typedef struct Cursor {
    Table* table;
    uint32_t page_index;
    uint32_t cell_index;
    bool found;
} Cursor;


Page* page_by_cursor(Cursor cursor) {
    return cursor.table->pages[cursor.page_index];
}

Record* cursor_value(Cursor cursor) {
    Page* page = page_by_cursor(cursor);
    return &page->leaf.records[cursor.cell_index];
}

Cursor leaf_node_find(Table* table, uint32_t page_index, const char* key) {
    Page* page = get_page(table, page_index);
    Cursor cursor = {.table = table, .page_index = page_index};

    // cursor may point to the index past the last element
    uint32_t lower = 0, upper = page->size;
    while (lower != upper) {
        uint32_t index = (lower + upper) / 2;
        const char* key_at_index = page->leaf.records[index].key;
        int cmp = strcmp(key, key_at_index);
        if (cmp == 0) {
            cursor.cell_index = index;
            cursor.found = true;
            return cursor;
        }
        if (cmp < 0) {
            upper = index;
        } else {
            lower = index + 1;
        }
    }
    cursor.cell_index = lower;
    cursor.found = false;
    return cursor;
}

uint32_t internal_node_find_child(Page* page, const char* key) {
    // there is one more child than key
    uint32_t lower = 0, upper = page->size;

    while (lower != upper) {
        uint32_t index = (lower + upper) / 2;
        const char* key_at_index = internal_key_of(page, index);
        int cmp = strcmp(key, key_at_index);
        if (cmp <= 0) {
            upper = index;
        } else {
            lower = index + 1;
        }
    }

    return lower;
}

Cursor internal_node_find(Table* table, uint32_t page_index, const char* key) {
    Page* page = get_page(table, page_index);
    uint32_t index = internal_node_find_child(page, key);
    uint32_t child_index = *internal_child_of(page, index);
    Page* child = get_page(table, child_index);

    switch (child->type) {
        case NODE_LEAF:
            return leaf_node_find(table, child_index, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_index, key);
    }
    unreachable();
}

Cursor table_find(Table* table, const char* key) {
    Page* root = get_page(table, ROOT_PAGE_INDEX);

    switch (root->type) {
        case NODE_LEAF:
            return leaf_node_find(table, ROOT_PAGE_INDEX, key);
        case NODE_INTERNAL:
            return internal_node_find(table, ROOT_PAGE_INDEX, key);
    }
    unreachable();
}

// insertion

void create_new_root(Table* table, uint32_t right_page_index) {
    uint32_t left_page_index = table->size;
    Page* root_page = get_page(table, ROOT_PAGE_INDEX);
    Page* left_page = get_page(table, left_page_index);
    Page* right_page = get_page(table, right_page_index);
    // keep page 0 the root page
    memcpy(left_page, root_page, PAGE_SIZE);
    left_page->is_root = false;
    initialize_internal_node(root_page);
    root_page->is_root = true;
    root_page->size = 1;
    *internal_child_of(root_page, 0) = left_page_index;
    strcpy(internal_key_of(root_page, 0), recursive_max_key_of(table, left_page));
    *internal_child_of(root_page, 1) = right_page_index;
    left_page->parent = ROOT_PAGE_INDEX;
    right_page->parent = ROOT_PAGE_INDEX;
}


void leaf_node_insert_raw(Cursor cursor, const char* key, const char* value) {
    Page* page = page_by_cursor(cursor);

    memmove(&page->leaf.records[cursor.cell_index + 1],
            &page->leaf.records[cursor.cell_index],
            sizeof(Record) * (page->size - cursor.cell_index));

    ++page->size;
    Record* record = &page->leaf.records[cursor.cell_index];
    strcpy(record->key, key);
    strcpy(record->value, value);
}

void update_internal_node_key(Page* page, const char* old_key, const char* new_key) {
    uint32_t old_child_index = internal_node_find_child(page, old_key);
    strcpy(internal_key_of(page, old_child_index), new_key);
}

void internal_node_insert_raw(Table* table, uint32_t parent_page_index, uint32_t child_page_index) {
    Page* parent_page = get_page(table, parent_page_index);
    Page* child_page = get_page(table, child_page_index);
    uint32_t old_size = parent_page->size;
    child_page->parent = parent_page_index;

    uint32_t* right_child_index = internal_rightmost_child_of(parent_page);
    Page* right_child = get_page(table, *right_child_index);
    const char* child_max_key = recursive_max_key_of(table, child_page);
    const char* right_max_key = recursive_max_key_of(table, right_child);

    if (strcmp(child_max_key, right_max_key) > 0) {
        ++parent_page->size;
        *internal_child_of(parent_page, old_size) = *right_child_index;
        strcpy(internal_key_of(parent_page, old_size), right_max_key);
        *right_child_index = child_page_index;
    } else {
        uint32_t index = internal_node_find_child(parent_page, child_max_key);
        ++parent_page->size;

        memmove(&parent_page->internal.branches[index + 1],
                &parent_page->internal.branches[index],
                sizeof(Branch) * (old_size - index));

        *internal_child_of(parent_page, index) = child_page_index;
        strcpy(internal_key_of(parent_page, index), child_max_key);
    }
}

void internal_node_insert(Table* table, uint32_t parent_page_index, uint32_t child_page_index);

void internal_node_split_and_insert(Table* table, uint32_t old_page_index, uint32_t child_page_index) {
    uint32_t new_page_index = table->size;
    Page* old_page = get_page(table, old_page_index);
    Page* new_page = get_page(table, new_page_index);
    initialize_internal_node(new_page);
    uint32_t parent_page_index = new_page->parent = old_page->parent;
    Page* child_page = get_page(table, child_page_index);
    const char* old_max_key = recursive_max_key_of(table, old_page);

    memcpy(&new_page->internal.branches[0],
           &old_page->internal.branches[INTERNAL_LEFT_SPLIT_SIZE],
           sizeof(Branch) * INTERNAL_RIGHT_SPLIT_SIZE);

    old_page->size = INTERNAL_LEFT_SPLIT_SIZE;
    new_page->size = INTERNAL_RIGHT_SPLIT_SIZE;

    *internal_rightmost_child_of(new_page) = *internal_rightmost_child_of(old_page);
    *internal_rightmost_child_of(old_page) = *internal_child_of(old_page, old_page->size - 1);
    const char* mid_key = internal_key_of(old_page, old_page->size - 1);
    --old_page->size;

    for (uint32_t i = 0; i <= new_page->size; ++i) {
        get_page(table, *internal_child_of(new_page, i))->parent = new_page_index;
    }

    uint32_t insert_to = (strcmp(recursive_max_key_of(table, child_page), mid_key) <= 0 ? old_page_index : new_page_index);
    internal_node_insert_raw(table, insert_to, child_page_index);

    if (old_page->is_root) {
        create_new_root(table, new_page_index);
        old_page_index = table->size - 1; // old page is now moved to a new place
        old_page = get_page(table, old_page_index);
        for (uint32_t i = 0; i <= old_page->size; ++i) {
            get_page(table, *internal_child_of(old_page, i))->parent = old_page_index;
        }
    } else {
        const char* new_max_key = recursive_max_key_of(table, old_page);
        Page* parent_page = get_page(table, parent_page_index);
        update_internal_node_key(parent_page, old_max_key, new_max_key);
        internal_node_insert(table, parent_page_index, new_page_index);
    }
}

void internal_node_insert(Table* table, uint32_t parent_page_index, uint32_t child_page_index) {
    Page* parent_page = get_page(table, parent_page_index);

    if (parent_page->size >= INTERNAL_BRANCH_SIZE) {
        internal_node_split_and_insert(table, parent_page_index, child_page_index);
    } else {
        internal_node_insert_raw(table, parent_page_index, child_page_index);
    }
}

void leaf_node_split_and_insert(Cursor cursor, const char* key, const char* value) {
    uint32_t old_page_index = cursor.page_index;
    uint32_t new_page_index = cursor.table->size;
    Page* old_page = page_by_cursor(cursor);
    Page* new_page = get_page(cursor.table, cursor.table->size);
    initialize_leaf_node(new_page);
    uint32_t parent_page_index = new_page->parent = old_page->parent;
    const char* old_max_key = recursive_max_key_of(cursor.table, old_page);

    memcpy(&new_page->leaf.records[0],
           &old_page->leaf.records[LEAF_LEFT_SPLIT_SIZE],
           sizeof(Record) * LEAF_RIGHT_SPLIT_SIZE);

    old_page->size = LEAF_LEFT_SPLIT_SIZE;
    new_page->size = LEAF_RIGHT_SPLIT_SIZE;
    bool in_new_page = cursor.cell_index > LEAF_LEFT_SPLIT_SIZE;
    Cursor new_cursor = {.table = cursor.table,
                         .page_index = (in_new_page ? new_page_index : old_page_index),
                         .cell_index = cursor.cell_index - (in_new_page ? LEAF_LEFT_SPLIT_SIZE : 0),
                         .found = cursor.found};
    leaf_node_insert_raw(new_cursor, key, value);

    if (old_page->is_root) {
        create_new_root(cursor.table, new_page_index);
    } else {
        const char* new_max_key = recursive_max_key_of(cursor.table, old_page);
        Page* parent_page = get_page(cursor.table, parent_page_index);
        update_internal_node_key(parent_page, old_max_key, new_max_key);
        internal_node_insert(cursor.table, parent_page_index, new_page_index);
    }
}


void leaf_node_insert(Cursor cursor, const char* key, const char* value) {
    Page* page = page_by_cursor(cursor);

    if (page->size >= LEAF_RECORD_SIZE) {
        leaf_node_split_and_insert(cursor, key, value);
    } else {
        leaf_node_insert_raw(cursor, key, value);
    }
}

// accessors

void db_set(Table* table, const char* key, const char* value) {
    validate_key(key);
    validate_value(value);

    pthread_mutex_lock(&table->mtx);

    Cursor cursor = table_find(table, key);
    Record* record = cursor_value(cursor);
    if (cursor.found) {
        // assign
        strcpy(record->value, value);
    } else {
        // insert
        leaf_node_insert(cursor, key, value);
    }

    pthread_mutex_unlock(&table->mtx);
}

const char* db_get(Table* table, const char* key) {
    validate_key(key);

    pthread_mutex_lock(&table->mtx);

    Cursor cursor = table_find(table, key);
    Record* record = cursor_value(cursor);
    const char* result;
    if (cursor.found) {
        result = record->value;
    } else {
        result = NULL;
    }

    pthread_mutex_unlock(&table->mtx);
    return result;
}

// test printer

void print_indent(uint32_t indent) {
    indent *= 2;
    while (indent--) putchar(' ');
}

void print_page(Table* table, uint32_t page_index, uint32_t indent) {
    Page* page = get_page(table, page_index);

    switch (page->type) {
        case NODE_LEAF: {
            print_indent(indent);
            printf("- leaf (size %d)\n", page->size);
            for (uint32_t i = 0; i < page->size; i++) {
                Record* record = &page->leaf.records[i];
                print_indent(indent + 1);
                printf("- key-value (%s -> %s)\n", record->key, record->value);
            }
            break;
        }
        case NODE_INTERNAL: {
            print_indent(indent);
            printf("- internal (size %d)\n", page->size);
            for (uint32_t i = 0; i < page->size; i++) {
                print_page(table, *internal_child_of(page, i), indent + 1);
                print_indent(indent + 1);
                printf("- key (%s) \n", internal_key_of(page, i));
            }
            print_page(table, *internal_rightmost_child_of(page), indent + 1);
            break;
        }
    }
}

void db_test_print(Table* table) {
    print_page(table, ROOT_PAGE_INDEX, 0);
}

