#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 5000
#define BUCKETS_NUM 100
#define FILE_NAME "data1.db"

#define CALL_HT_DELETE 200  // how many times to call HT_DELETE

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

  int indexDesc;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME, BUCKETS_NUM));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc));

  Record record;
  srand(12569874);
  int r;

  //printf("Initially: PrintAllEntries\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
  
  printf("Insert Entries\n");
  int id = 0;
  for (; id < RECORDS_NUM / 2; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    
    printf("\nPrint Entry with id = %d:\n", id);  // must print nothing at all
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
    
    printf("Insert Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
    
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
  }
  
  //printf("After Insertions: PrintAllEntries\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
  
  int i = 1;
  for (; i <= CALL_HT_DELETE / 2; i++) {
  	int id = rand() % RECORDS_NUM;
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));

    printf("Delete Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_DeleteEntry(indexDesc, id));
  
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));  // must print nothing at all
  }
  
  //printf("After Deletions: PrintAllEntries\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
  
  printf("Insert Entries\n");
  for (; id < RECORDS_NUM; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    
    printf("\nPrint Entry with id = %d:\n", id);  // must print nothing at all
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
    
    printf("Insert Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
    
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
  }
  
  //printf("After Insertions: PrintAllEntries\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
  
  for (; i <= CALL_HT_DELETE; i++) {
  	int id = rand() % RECORDS_NUM;
    printf("\nPrint Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));

    printf("Delete Entry with id = %d.\n" ,id);
    CALL_OR_DIE(HT_DeleteEntry(indexDesc, id));
  
    printf("Print Entry with id = %d:\n", id);
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));  // must print nothing at all
  }
  
  //printf("After Deletions: PrintAllEntries\n");
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
  
  CALL_OR_DIE(HT_CloseFile(indexDesc));
  BF_Close();
}
