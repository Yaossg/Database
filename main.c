#include "database.h"

void test_single_threaded() {
    remove("db.db");
    Table* table = db_open("db.db");
    for (int i = 0; i <= 100; ++i) {
        KEY_TYPE key;
        VALUE_TYPE value;
        sprintf(key, "hello%d", i);
        sprintf(value, "world%d", i);
        db_set(table, key, value);
    }
    for (int i = 0; i <= 100; ++i) {
        KEY_TYPE key;
        VALUE_TYPE value;
        sprintf(key, "hello%d", i);
        sprintf(value, "world%d", i);
        assert(strcmp(db_get(table, key), value) == 0);
    }
    db_test_print(table);
    db_close(table);
}

Table* table;

void* test_thread(void* args) {
    for (int i = *(int*)args; i <= 100; i += 2) {
        KEY_TYPE key;
        VALUE_TYPE value;
        sprintf(key, "hello%d", i);
        sprintf(value, "world%d", i);
        db_set(table, key, value);
    }
    return NULL;
}

void test_multithreaded() {
    remove("db.db");
    table = db_open("db.db");

    pthread_t t1, t2;
    pthread_create(&t1, NULL, test_thread, &(int){0});
    pthread_create(&t2, NULL, test_thread, &(int){1});
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    for (int i = 0; i <= 100; ++i) {
        KEY_TYPE key;
        VALUE_TYPE value;
        sprintf(key, "hello%d", i);
        sprintf(value, "world%d", i);
        assert(strcmp(db_get(table, key), value) == 0);
    }
    db_test_print(table);
    db_close(table);
}

void test_persistence() {
    remove("db.db");
    Table* table = db_open("db.db");
    for (int i = 0; i <= 100; ++i) {
        KEY_TYPE key;
        VALUE_TYPE value;
        sprintf(key, "hello%d", i);
        sprintf(value, "world%d", i);
        db_set(table, key, value);
    }
    db_close(table);
    table = db_open("db.db");
    for (int i = 0; i <= 100; ++i) {
        KEY_TYPE key;
        VALUE_TYPE value;
        sprintf(key, "hello%d", i);
        sprintf(value, "world%d", i);
        assert(strcmp(db_get(table, key), value) == 0);
    }
    db_test_print(table);
    db_close(table);
}

int main() {
    test_single_threaded();
    test_multithreaded();
    test_persistence();
}
