#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

#define INVALID -1

#define EMPTY_DESC -2

#define EMPTY_BUCKET -3

#define NO_LINK -4

int file_descs[MAX_OPEN_FILES];  /* A global array that contains the descriptors of all the open files */

int Hash(int id, int buckets_num) {
  return id % buckets_num;  /* Implemented as modulo */
}

HT_ErrorCode HT_Init() {
  int i = 0;  /* Starting from the first slot */
  for (; i <= MAX_OPEN_FILES - 1; i++)
  	file_descs[i] = EMPTY_DESC;  /* Initialize each slot of the global array as empty descriptor */
  return HT_OK;  /* Success */
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {
  CALL_BF(BF_CreateFile(filename));  /* Create a file using BF level */
  int indexDesc = INVALID;  /* Index in the global array file_descs */
  int i = 0;  /* Starting from the first slot */
  for (; i <= MAX_OPEN_FILES - 1; i++) {
  	if (file_descs[i] == EMPTY_DESC)  /* If an empty slot is found */
  	  indexDesc = i;  /* Use it */
  }
  if (indexDesc == INVALID)  /* There was not any empty slot */
  	return HT_ERROR;  /* The maximum number of open files has been reached */
  CALL_BF(BF_OpenFile(filename, &file_descs[indexDesc]));  /* Open the file and set file decriptor to a valid value using BF level */
  BF_Block *first_block;  /* Block pointer for memory allocation and management */
  BF_Block_Init(&first_block);  /* Allocate and initialize memory for a block */
  CALL_BF(BF_AllocateBlock(file_descs[indexDesc], first_block));  /* Allocate memory for this block inside the open file */
  char *file_type = BF_Block_GetData(first_block);  /* Find where the block's data starts */
  char *set_type = "hash";  /* "hash" stands for hash file */
  memcpy(file_type, set_type, strlen(set_type) + 1);  /* Set the type of the file */
  int *buckets_num = (int *)(file_type + strlen(file_type) + 1);  /* Buckets number is right after the file type */
  *buckets_num = buckets;  /* Set it as the given value */
  BF_Block_SetDirty(first_block);  /* Changed data so mark block as dirty */
  CALL_BF(BF_UnpinBlock(first_block));  /* Unpin block */
  BF_Block_Destroy(&first_block);  /* Free the allocated memory */
  CALL_BF(BF_CloseFile(file_descs[indexDesc]));  /* Close the file using BF level */
  return HT_OK;  /* Success */
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  int fileDesc; /* The file descriptor of the file */
  CALL_BF(BF_OpenFile(fileName, &fileDesc));  /* Open the file and set fileDesc to a valid value using BF level */
  int i = 0;  /* Starting from the first slot */
  for (; i <= MAX_OPEN_FILES - 1; i++) {
  	if (file_descs[i] == fileDesc)  /* If this file descriptor is found */
  	  *indexDesc = i;  /* Save its index */
  }
  BF_Block *first_block;  /* Block pointer for memory allocation and management */
  BF_Block_Init(&first_block);  /* Allocate and initialize memory for a block */
  CALL_BF(BF_GetBlock(fileDesc, 0, first_block));  /* Get the first block of the open file using BF level (zero indexing) */
  char *file_type = BF_Block_GetData(first_block);  /* The file type is at the start of first block's data */
  if (strcmp(file_type, "hash") != 0) {  /* Check if it is a hash file */
  	CALL_BF(BF_UnpinBlock(first_block));  /* Unpin block */
    BF_Block_Destroy(&first_block);  /* Free the allocated memory */
  	return HT_ERROR;  /* It is not a hash file */
  }
  /* Data did not change, so block is not dirty */
  CALL_BF(BF_UnpinBlock(first_block));  /* Unpin block */
  BF_Block_Destroy(&first_block);  /* Free the allocated memory */
  return HT_OK;  /* Success */
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  CALL_BF(BF_CloseFile(file_descs[indexDesc]));  /* Close the file using BF level */
  return HT_OK;  /* Success */
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  int blocks_num;  /* Number of blocks in the open file */
  CALL_BF(BF_GetBlockCounter(file_descs[indexDesc], &blocks_num));  /* Get that number using BF level */
  BF_Block *first_block;  /* Block pointer for memory allocation and management */
  BF_Block_Init(&first_block);  /* Allocate and initialize memory for a block */
  CALL_BF(BF_GetBlock(file_descs[indexDesc], 0, first_block));  /* Get the first block of this file using BF level */
  char *file_type = BF_Block_GetData(first_block);  /* The file type is at the start of first block's data */
  int buckets_num = *(int *)(file_type + strlen(file_type) + 1);  /* Buckets number is right after the file type */
  CALL_BF(BF_UnpinBlock(first_block));  /* Unpin first block */
  BF_Block_Destroy(&first_block);  /* Free the allocated memory */
  if (buckets_num == 0)  /* If buckets number of this hash file is zero */
    return HT_ERROR;  /* There are not enough buckets to insert the entry */
  BF_Block *map_block;  /* Block pointer for memory allocation and management */
  BF_Block_Init(&map_block);  /* Allocate and initialize memory for a block */
  int blocks_for_map = (buckets_num * sizeof(int)) / (BF_BLOCK_SIZE - 2 * sizeof(int)) + 1;  /* Number of blocks needed for map */
  int buckets_in_last_map_block = (buckets_num * sizeof(int)) % (BF_BLOCK_SIZE - 2 * sizeof(int));  /* Number of buckets in last map's block */
  if (buckets_in_last_map_block == 0)  /* If there is not any bucket in last map's block */
    blocks_for_map--;  /* Then one less block is actually needed */
  int buckets_in_each_block;  /* Number of buckets in each map's block (except the last one) */
  if (blocks_for_map > 1)  /* If more than one blocks are needed for map */
    buckets_in_each_block = (buckets_num - buckets_in_last_map_block) / (blocks_for_map - 1);  /* Calculate how many buckets are in each map's block */
  else {  /* Map consists of only one block */
    buckets_in_each_block = buckets_num;  /* There is only one block for all the buckets of the file */
    buckets_in_last_map_block = buckets_num;  /* This block is also the last one of the map */
  }
  if (blocks_num == 1) {  /* If there was only one block in the file, that means the map has not been created yet */
    int block = 1;  /* Skip the first block */
    for (; block <= blocks_for_map; block++) {  /* Build the initially empty map */
      CALL_BF(BF_AllocateBlock(file_descs[indexDesc], map_block));  /* Allocate memory for another block inside the open file */
      int *bucket_counter = (int *)BF_Block_GetData(map_block);  /* First data is the number of buckets in this block */
      int *next_map_block = bucket_counter + 1;  /* Right after the bucket_counter is the number of next map's block */
      int *buckets = next_map_block + 1;  /* After that the remaining space is for buckets */
      int bucket = 0;  /* Starting from the first bucket slot in this block */
      if (block < blocks_for_map) {  /* If the block is not the last one of the map */
      	*bucket_counter = buckets_in_each_block;  /* Set the counter as the calculated value of buckets_in_each_block */
      	*next_map_block = block + 1;  /* The next map's block is going to be the next one in the file */
      	for (; bucket <= buckets_in_each_block - 1; bucket++) {  /* For each bucket in this block */
      	  int *current_bucket = buckets + bucket;  /* Find where it starts */
          *current_bucket = EMPTY_BUCKET;  /* Initialize it as an bucket */
        }
      }
      else {  /* If the block is the last one of the map */
        *bucket_counter = buckets_in_last_map_block;  /* Set the counter as the calculated value of buckets_in_last_map_block */
        *next_map_block = NO_LINK;  /* There is not a next map's block, because this is the last one */
        int *current_bucket = buckets + bucket;  /* Find where it starts */
        *current_bucket = EMPTY_BUCKET;  /* Initialize it as an bucket */
        for (; bucket <= buckets_in_last_map_block - 1; bucket++) {  /* For each bucket in this block */
      	  int *current_bucket = buckets + bucket;  /* Find where it starts */
          *current_bucket = EMPTY_BUCKET;  /* Initialize it as an bucket */
        }
      }
      BF_Block_SetDirty(map_block);  /* Changed data so set map's block dirty*/
      CALL_BF(BF_UnpinBlock(map_block));  /* Unpin map's block */
    }
  }
  Record *rec = (Record *)malloc(sizeof(Record));  /* Record pointer for memory allocation and management */
  if (rec == NULL) {  /* If a problem occured */
    BF_Block_Destroy(&map_block);  /* Free the previously allocated memory */
    return HT_ERROR;  /* Allocation failed */
  }
  *rec = record;  /* Set the pointer's content as the record that is going to be added */
  int bucket_to_insert = Hash(rec->id, buckets_num);  /* Hash id to find in which bucket to insert the record */
  int map_block_to_insert = bucket_to_insert / buckets_in_each_block;  /* Find in which map's block there is the bucket */
  CALL_BF(BF_GetBlock(file_descs[indexDesc], map_block_to_insert + 1, map_block));  /* Get that block */
  int *bucket_counter = (int *)BF_Block_GetData(map_block);  /* First data is the number of buckets in this block */
  int *next_map_block = bucket_counter + 1;  /* Right after the bucket_counter is the number of next map's block */
  int *buckets = next_map_block + 1;  /* After that the remaining space is for buckets */
  int *target_bucket = buckets + (bucket_to_insert % buckets_in_each_block);  /* Find where the target bucket is in this block */
  if (*target_bucket == EMPTY_BUCKET) {  /* If the target bucket is empty */
    BF_Block *new_block;  /* Block pointer for memory allocation and management */
    BF_Block_Init(&new_block);  /* Allocate and initialize memory for a block */
    CALL_BF(BF_AllocateBlock(file_descs[indexDesc], new_block));  /* Allocate memory for a new block inside the open file */
    int *record_counter = (int *)BF_Block_GetData(new_block);  /* First data is the number of records in this block */
    int *overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
    Record *data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
    *record_counter = 1;  /* The first record is going to be added to this block, so set its record_counter to 1 */
    *overflow_block = NO_LINK;  /* At this point, an overflow block is not needed yet */
    memcpy(data, rec, sizeof(Record));  /* Copy the record to the records' space of the block */
    BF_Block_SetDirty(new_block);  /* Changed data so set new block dirty*/
    CALL_BF(BF_UnpinBlock(new_block));  /* Unpin the new block */
    BF_Block_Destroy(&new_block);  /* Free the allocated memory */
    int current_block_num;  /* Current number of blocks in the file */
    CALL_BF(BF_GetBlockCounter(file_descs[indexDesc], &current_block_num));  /* Get that number using BF level */
    *target_bucket = current_block_num - 1;  /* This is going to be the first block of this bucket now on */
    BF_Block_SetDirty(map_block);  /* Changed data so set map's block dirty */
  }
  else {  /* If the target bucket is not empty (there are corresponding blocks) */
  	BF_Block *block;  /* Block pointer for memory allocation and management */
    BF_Block_Init(&block);  /* Allocate and initialize memory for a block */
    CALL_BF(BF_GetBlock(file_descs[indexDesc], *target_bucket, block));  /* Get the first block of this bucket */
    int *record_counter = (int *)BF_Block_GetData(block);  /* First data is the number of records in this block */
    int *overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
    while (((*record_counter) + 1) * sizeof(Record) > BF_BLOCK_SIZE - 2 * sizeof(int)) {  /* While there is not enough space in the current block for another record */
      if (*overflow_block == NO_LINK) {  /* If the current block (which is full) has not an overflow block */
      	int current_block_num;  /* Current number of blocks in the file */
        CALL_BF(BF_GetBlockCounter(file_descs[indexDesc], &current_block_num));  /* Get that number using BF level */
        *overflow_block = current_block_num;  /* Set that number as an overflow block (a new block is going to be created) */
        BF_Block_SetDirty(block);  /* Changed data so set the existing block dirty*/
        CALL_BF(BF_UnpinBlock(block));  /* Unpin the existing block */
        BF_Block_Destroy(&block);  /* Free the allocated memory */
        BF_Block *new_block;  /* Block pointer for memory allocation and management */
        BF_Block_Init(&new_block);  /* Allocate and initialize memory for a block */
        CALL_BF(BF_AllocateBlock(file_descs[indexDesc], new_block));  /* Allocate memory for a new block inside the open file */
        int *record_counter = (int *)BF_Block_GetData(new_block);  /* First data is the number of records in this block */
        int *overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
        Record *data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
        *record_counter = 1;  /* The first record is going to be added to this block, so set its record_counter to 1 */
        *overflow_block = NO_LINK;  /* At this point, an overflow block is not needed yet */
        memcpy(data, rec, sizeof(Record));  /* Copy the record to the records' space of the block */
        BF_Block_SetDirty(new_block);  /* Changed data so set new block dirty*/
        CALL_BF(BF_UnpinBlock(new_block));  /* Unpin new block */
        BF_Block_Destroy(&new_block);  /* Free the allocated memory */
        free(rec);  /* Free the allocated memory */
        CALL_BF(BF_UnpinBlock(map_block));  /* Unpin map's block */
        BF_Block_Destroy(&map_block);  /* Free the previously allocated memory */
        return HT_OK;  /* Success */
      }
      CALL_BF(BF_UnpinBlock(block));  /* Unpin the current block */
      CALL_BF(BF_GetBlock(file_descs[indexDesc], *overflow_block, block));  /* Get its overflow block */
      record_counter = (int *)BF_Block_GetData(block);  /* Update the record_counter pointer */
      overflow_block = record_counter + 1;  /* Update the overflow_block pointer, which is right after record_counter */
    }
    Record *data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
    data += *record_counter;  /* Go to the empty space of this block */
    (*record_counter)++;  /* A record is going to be added, so increase the record_counter by 1 */
    memcpy(data, rec, sizeof(Record));  /* Copy record to the records' space of the block */
    BF_Block_SetDirty(block);  /* Changed data so set block dirty */
    CALL_BF(BF_UnpinBlock(block));  /* Unpin block */
    BF_Block_Destroy(&block);  /* Free the allocated memory */
  }
  free(rec);  /* Free the allocated memory */
  CALL_BF(BF_UnpinBlock(map_block));  /* Unpin map's block */
  BF_Block_Destroy(&map_block);  /* Free the previously allocated memory */
  return HT_OK;  /* Success */
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  int blocks_num;  /* Number of blocks in the hash file */
  CALL_BF(BF_GetBlockCounter(file_descs[indexDesc], &blocks_num));  /* Get that number using BF level */
  if (blocks_num == 0)  /* If there are not any blocks */
    return HT_ERROR;  /* This hash file has never been created */
  else if (blocks_num == 1)  /* Else if there is only one block */
    return HT_OK;  /* Fine, but there are not any records to print */
  BF_Block *map_block;  /* Block pointer for memory allocation and management */
  BF_Block_Init(&map_block);  /* Allocate and initialize memory for a block */
  if (id == NULL) {  /* This means that all records need to be printed regardless */
    CALL_BF(BF_GetBlock(file_descs[indexDesc], 1, map_block));  /* Get first map's block using BF level */
    int *bucket_counter = (int *)BF_Block_GetData(map_block);  /* First data is the number of buckets in this block */
    int *next_map_block = bucket_counter + 1;  /* Right after the bucket_counter is the number of next map's block */
    int *buckets = next_map_block + 1;  /* After that the remaining space is for buckets */
    BF_Block *block;  /* Block pointer for memory allocation and management */
    BF_Block_Init(&block);  /* Allocate and initialize memory for a block */
    while (1) {  /* A terminal condition for this loop is going to be checked later */
      int bucket = 0;  /* Starting from the first bucket of the current map's block */
      for (; bucket <= *bucket_counter - 1; bucket++) {  /* For each bucket in this block */
        int *current_bucket = buckets + bucket;  /* Find where this bucket starts */
      	if (*current_bucket != EMPTY_BUCKET) {  /* If the bucket is not empty */
      	  CALL_BF(BF_GetBlock(file_descs[indexDesc], *current_bucket, block));  /* Get the first block of the current bucket */
          int *record_counter = (int *)BF_Block_GetData(block);  /* First data is the number of records in this block */
          int *overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
          Record *data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
          while(1) {  /* A terminal condition for this loop is going to be checked later */
            int record = 0;  /* Starting from the first record in this block */
            for (; record <= *record_counter - 1; record++) {  /* For each record in this block */
          	  Record *rec = data + record;  /* Find where it is */
          	  printf("%d,\"%s\",\"%s\",\"%s\"\n", rec->id, rec->name, rec->surname, rec->city);  /* Print this record */
            }
            if (*overflow_block == NO_LINK) {  /* Repeat until there is not an overflow block (terminal condition) */
              break;
            }
            CALL_BF(BF_UnpinBlock(block));  /* Unpin block */
      	  	CALL_BF(BF_GetBlock(file_descs[indexDesc], *overflow_block, block));  /* Get its overflow block of the file */
      	    record_counter = (int *)BF_Block_GetData(block);  /* First data is the number of records in this block */
            overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
            data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
          }
          CALL_BF(BF_UnpinBlock(block));  /* Unpin the last block of the current bucket */
        }
      }
      if (*next_map_block == NO_LINK) {  /* Repeat until the last block of the map (terminal condition) */
      	CALL_BF(BF_UnpinBlock(map_block));  /* Unpin map's block */
      	break;
      }
      CALL_BF(BF_UnpinBlock(map_block));  /* Unpin map's block */
      CALL_BF(BF_GetBlock(file_descs[indexDesc], *next_map_block, map_block));  /* Get its next map's block */
      bucket_counter = (int *)BF_Block_GetData(map_block);  /* Update bucket_number pointer */
      next_map_block = bucket_counter + 1;  /* Update the next_map_block pointer, which is right after record_counter */
      buckets = next_map_block + 1;  /* Update buckets pointer */
    }
    BF_Block_Destroy(&block);  /* Free the allocated memory */
  }
  else {
  	BF_Block *first_block;  /* Block pointer for memory allocation and management */
    BF_Block_Init(&first_block);  /* Allocate and initialize memory for a block */
    CALL_BF(BF_GetBlock(file_descs[indexDesc], 0, first_block));  /* Get the first block of this file using BF level */
    char *file_type = BF_Block_GetData(first_block);  /* The file type is at the start of first block's data */
    int buckets_num = *(int *)(file_type + strlen(file_type) + 1);  /* Buckets number is right after the file type */
    CALL_BF(BF_UnpinBlock(first_block));  /* Unpin first block */
    BF_Block_Destroy(&first_block);  /* Free the allocated memory */
    int blocks_for_map = (buckets_num * sizeof(int)) / (BF_BLOCK_SIZE - 2 * sizeof(int)) + 1;  /* Number of blocks needed for map */
    int buckets_in_last_map_block = (buckets_num * sizeof(int)) % (BF_BLOCK_SIZE - 2 * sizeof(int));  /* Number of buckets in last map's block */
    if (buckets_in_last_map_block == 0)  /* If there is not any bucket in last map's block */
      blocks_for_map--;  /* Then one less block is actually needed */
    int buckets_in_each_block;  /* Number of buckets in each map's block (except the last one) */
    if (blocks_for_map > 1)  /* If more than one blocks are needed for map */
      buckets_in_each_block = (buckets_num - buckets_in_last_map_block) / (blocks_for_map - 1);  /* Calculate how many buckets are in each map's block */
    else  /* Map consists of only one block */
      buckets_in_each_block = buckets_in_last_map_block;  /* This block is also the last one of the map */
  	int bucket_to_insert = Hash(*id, buckets_num);  /* Hash id to find in which bucket to insert the record */
    int map_block_to_insert = bucket_to_insert / buckets_in_each_block;  /* Find in which map's block there is the bucket */
    CALL_BF(BF_GetBlock(file_descs[indexDesc], map_block_to_insert + 1, map_block));  /* Get that block */
    int *bucket_counter = (int *)BF_Block_GetData(map_block);  /* First data is the number of buckets in this block */
    int *next_map_block = bucket_counter + 1;  /* Right after the bucket_counter is the number of next map's block */
    int *buckets = next_map_block + 1;  /* After that the remaining space is for buckets */
    int *target_bucket = buckets + (bucket_to_insert % buckets_in_each_block);  /* Find where the target bucket is in this block */
    BF_Block *block;  /* Block pointer for memory allocation and management */
    BF_Block_Init(&block);  /* Allocate and initialize memory for a block */
    if (*target_bucket != EMPTY_BUCKET) {  /* If the target bucket is not empty */
  	  CALL_BF(BF_GetBlock(file_descs[indexDesc], *target_bucket, block));  /* Get the first block of the target bucket */
      int *record_counter = (int *)BF_Block_GetData(block);  /* First data is the number of records in this block */
      int *overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
      Record *data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
      while(1) {  /* A terminal condition for this loop is going to be checked later */
        int record = 0;  /* Starting from the first record in this block */
        for (; record <= *record_counter - 1; record++) {  /* For each record in this block */
      	  Record *rec = data + record;  /* Find where it is */
      	  if (rec->id == *id)  /* If its id is the requested one */
            printf("%d,\"%s\",\"%s\",\"%s\"\n", rec->id, rec->name, rec->surname, rec->city);  /* Print this record */
        }
        if (*overflow_block == NO_LINK) {  /* Repeat until there is not an overflow block */
          break;
        }
        CALL_BF(BF_UnpinBlock(block));  /* Unpin block */
  	  	CALL_BF(BF_GetBlock(file_descs[indexDesc], *overflow_block, block));  /* Get its overflow block of the file */
  	    record_counter = (int *)BF_Block_GetData(block);  /* First data is the number of records in this block */
        overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
        data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
      }
      CALL_BF(BF_UnpinBlock(block));  /* Unpin the last block of the current bucket */
    }
    BF_Block_Destroy(&block);  /* Free the allocated memory */
  }
  CALL_BF(BF_UnpinBlock(map_block));  /* Unpin map's block */
  BF_Block_Destroy(&map_block);  /* Free the previously allocated memory */
  return HT_OK;  /* Success */
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
  BF_Block *first_block;  /* Block pointer for memory allocation and management */
  BF_Block_Init(&first_block);  /* Allocate and initialize memory for a block */
  CALL_BF(BF_GetBlock(file_descs[indexDesc], 0, first_block));  /* Get the first block of this file using BF level */
  char *file_type = BF_Block_GetData(first_block);  /* The file type is at the start of first block's data */
  int buckets_num = *(int *)(file_type + strlen(file_type) + 1);  /* Buckets number is right after the file type */
  CALL_BF(BF_UnpinBlock(first_block));  /* Unpin first block */
  BF_Block_Destroy(&first_block);  /* Free the allocated memory */
  int blocks_for_map = (buckets_num * sizeof(int)) / (BF_BLOCK_SIZE - 2 * sizeof(int)) + 1;  /* Number of blocks needed for map */
  int buckets_in_last_map_block = (buckets_num * sizeof(int)) % (BF_BLOCK_SIZE - 2 * sizeof(int));  /* Number of buckets in last map's block */
  if (buckets_in_last_map_block == 0)  /* If there is not any bucket in last map's block */
    blocks_for_map--;  /* Then one less block is actually needed */
  int buckets_in_each_block;  /* Number of buckets in each map's block (except the last one) */
  if (blocks_for_map > 1)  /* If more than one blocks are needed for map */
    buckets_in_each_block = (buckets_num - buckets_in_last_map_block) / (blocks_for_map - 1);  /* Calculate how many buckets are in each map's block */
  else  /* Map consists of only one block */
    buckets_in_each_block = buckets_in_last_map_block;  /* This block is also the last one of the map */
  int bucket_to_insert = Hash(id, buckets_num);  /* Hash id to find in which bucket to insert the record */
  int map_block_to_insert = bucket_to_insert / buckets_in_each_block;  /* Find in which map's block there is the bucket */
  BF_Block *map_block;  /* Block pointer for memory allocation and management */
  BF_Block_Init(&map_block);  /* Allocate and initialize memory for a block */
  CALL_BF(BF_GetBlock(file_descs[indexDesc], map_block_to_insert + 1, map_block));  /* Get that block */
  int *bucket_counter = (int *)BF_Block_GetData(map_block);  /* First data is the number of buckets in this block */
  int *next_map_block = bucket_counter + 1;  /* Right after the bucket_counter is the number of next map's block */
  int *buckets = next_map_block + 1;  /* After that the remaining space is for buckets */
  int *target_bucket = buckets + (bucket_to_insert % buckets_in_each_block);  /* Find where the target bucket is in this block */
  BF_Block *block;  /* Block pointer for memory allocation and management */
  BF_Block_Init(&block);  /* Allocate and initialize memory for a block */
  if (*target_bucket != EMPTY_BUCKET) {  /* If the target bucket is not empty */
    CALL_BF(BF_GetBlock(file_descs[indexDesc], *target_bucket, block));  /* Get the first block of the target bucket */
    int *record_counter = (int *)BF_Block_GetData(block);  /* First data is the number of records in this block */
    int *overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
    Record *data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
    while (1) {
      int record = 0;  /* Starting from the first record in this block */
      for (; record <= *record_counter - 1; record++) {  /* For each record in this block */
  	    Record *rec = data + record;  /* Find where it is */
  	    if (rec->id == id) {  /* If its id is the requested one */
  	      if (*record_counter > 1 && record < *record_counter - 1) {  /* If it is not neither the only nor the last record in this block */
  	      	Record *last_record_in_this_block = data + *record_counter - 1;  /* Find the last record in this block */
            memcpy(rec, last_record_in_this_block, sizeof(Record));  /* And use it to overwrite the record that needs to be deleted*/
          }
          (*record_counter)--;  /* A record of this block has been deleted, so decrease the number of records by 1 */
          break;
        }
      }
      if (*overflow_block == NO_LINK) {  /* Repeat until there is not an overflow block */
        break;
      }
      CALL_BF(BF_UnpinBlock(block));  /* Unpin block */
  	  CALL_BF(BF_GetBlock(file_descs[indexDesc], *overflow_block, block));  /* Get its overflow block of the file */
      record_counter = (int *)BF_Block_GetData(block);  /* First data is the number of records in this block */
      overflow_block = record_counter + 1;  /* Right after that is the number of the overflow block */
      data = (Record *)(overflow_block + 1);  /* Skip overflow_block to get to the records' space */
    }
    CALL_BF(BF_UnpinBlock(block));  /* Unpin the last block of the current bucket */
  }
  BF_Block_Destroy(&block);  /* Free the allocated memory */
  CALL_BF(BF_UnpinBlock(map_block));  /* Unpin map's block */
  BF_Block_Destroy(&map_block);  /* Free the previously allocated memory */
  return HT_OK;  /* Success */
}
