#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 10000

#define BUCKETS_NUM_a 3
#define FILE_NAME_a "data2a.db"
#define CALL_HT_DELETE_a 50  // how many times to call HT_DELETE for file data2a.db

#define BUCKETS_NUM_b 17
#define FILE_NAME_b "data2b.db"
#define CALL_HT_DELETE_b 100  // how many times to call HT_DELETE for file data2b.db

#define BUCKETS_NUM_c 39
#define FILE_NAME_c "data2c.db"
#define CALL_HT_DELETE_c 150  // how many times to call HT_DELETE for file data2c.db

#define BUCKETS_NUM_d 56
#define FILE_NAME_d "data2d.db"
#define CALL_HT_DELETE_d 200  // how many times to call HT_DELETE for file data2d.db

#define BUCKETS_NUM_e 131
#define FILE_NAME_e "data2e.db"
#define CALL_HT_DELETE_e 250  // how many times to call HT_DELETE for file data2e.db

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
  BF_Init(LRU);
  
  CALL_OR_DIE(HT_Init());

  int indexDesc_a;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_a, BUCKETS_NUM_a));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_a, &indexDesc_a));
  
  int indexDesc_b;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_b, BUCKETS_NUM_b));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_b, &indexDesc_b));
  
  int indexDesc_c;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_c, BUCKETS_NUM_c));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_c, &indexDesc_c));
  
  int indexDesc_d;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_d, BUCKETS_NUM_d));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_d, &indexDesc_d));
  
  int indexDesc_e;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_e, BUCKETS_NUM_e));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_e, &indexDesc_e));
  
  Record record;
  srand(12569874);
  int r;

  //printf("\nPrintAllEntries of a:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_a, NULL));
  //printf("\nPrintAllEntries of b:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_b, NULL));
  //printf("\nPrintAllEntries of c:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_c, NULL));
  //printf("\nPrintAllEntries of d:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_d, NULL));
  //printf("\nPrintAllEntries of e:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_e, NULL));
  
  printf("Insert Entries\n");
  int id = 0;
  for (; id < RECORDS_NUM / 5; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_a, &id));  // must print nothing at all
    
    printf("Insert Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_InsertEntry(indexDesc_a, record));
    
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_a, &id));
  }
  
  for (; id < 2 * RECORDS_NUM / 5; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_b, &id));    // must print nothing at all
    
    printf("Insert Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_InsertEntry(indexDesc_b, record));
    
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_b, &id));
  }
  
  for (; id < 3 * RECORDS_NUM / 5; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_c, &id));    // must print nothing at all
    
    printf("Insert Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_InsertEntry(indexDesc_c, record));
    
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_c, &id));
  }
  
  for (; id < 4 * RECORDS_NUM / 5; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_d, &id));    // must print nothing at all
    
    printf("Insert Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_InsertEntry(indexDesc_d, record));
    
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_d, &id));
  }
  
  for (; id < RECORDS_NUM; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_e, &id));    // must print nothing at all
    
    printf("Insert Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_InsertEntry(indexDesc_e, record));
    
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_e, &id));
  }
  
  //printf("\nPrintAllEntries of a:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_a, NULL));
  //printf("\nPrintAllEntries of b:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_b, NULL));
  //printf("\nPrintAllEntries of c:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_c, NULL));
  //printf("\nPrintAllEntries of d:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_d, NULL));
  //printf("\nPrintAllEntries of e:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_e, NULL));
  
  int a = 1;
  for (; a <= CALL_HT_DELETE_a; a++) {
  	int id = rand() % RECORDS_NUM;
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_a, &id));  // might print nothing at all (if the record was inserted to another file)

    printf("Delete Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_DeleteEntry(indexDesc_a, id));
  
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_a, &id)); // must print nothing at all
  }
  
  int b = 1;
  for (; b <= CALL_HT_DELETE_b; b++) {
  	int id = rand() % RECORDS_NUM;
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_b, &id));  // might print nothing at all (if the record was inserted to another file)

    printf("Delete Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_DeleteEntry(indexDesc_b, id));
  
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_b, &id)); // must print nothing at all
  }
  
  int c = 1;
  for (; c <= CALL_HT_DELETE_c; c++) {
  	int id = rand() % RECORDS_NUM;
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_c, &id));  // might print nothing at all (if the record was inserted to another file)

    printf("Delete Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_DeleteEntry(indexDesc_c, id));
  
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_c, &id)); // must print nothing at all
  }
  
  int d = 1;
  for (; d <= CALL_HT_DELETE_d; d++) {
  	int id = rand() % RECORDS_NUM;
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_d, &id));  // might print nothing at all (if the record was inserted to another file)

    printf("Delete Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_DeleteEntry(indexDesc_d, id));
  
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_d, &id)); // must print nothing at all
  }
  
  int e = 1;
  for (; e <= CALL_HT_DELETE_e; e++) {
  	int id = rand() % RECORDS_NUM;
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_e, &id));  // might print nothing at all (if the record was inserted to another file)

    printf("Delete Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_DeleteEntry(indexDesc_e, id));
  
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc_e, &id)); // must print nothing at all
  }
  
  //printf("\nPrintAllEntries of a:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_a, NULL));
  //printf("\nPrintAllEntries of b:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_b, NULL));
  //printf("\nPrintAllEntries of c:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_c, NULL));
  //printf("\nPrintAllEntries of d:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_d, NULL));
  //printf("\nPrintAllEntries of e:\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc_e, NULL));
  
  CALL_OR_DIE(HT_CloseFile(indexDesc_a));
  CALL_OR_DIE(HT_CloseFile(indexDesc_b));
  CALL_OR_DIE(HT_CloseFile(indexDesc_c));
  CALL_OR_DIE(HT_CloseFile(indexDesc_d));
  CALL_OR_DIE(HT_CloseFile(indexDesc_e));
  
  BF_Close();
}
