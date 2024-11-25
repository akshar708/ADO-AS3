#include <limits.h>
#include "buffer_mgr.h"
#include "hash_table.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <stdio.h>

/* Additional Definitions */

#define PAGE_TABLE_SIZE 256
#define RC_OK 0


typedef unsigned int TimeStamp;
typedef struct BM_PageFrame {
    // the frame's buffer
    char* data;
    TimeStamp timeStamp;
    // the page currently occupying it
    PageNumber pageNum;
    // management data on the page frame
    bool occupied;
    int fixedCount;
    int framedIndex;
    bool dirty; 
} BM_PageFrame; 



typedef struct BM_Metadata {
     // an array of frames
    BM_PageFrame *pageFrames;
    // used to treat *pageFrames as a queue
    // a page table that associates the a page ID with an index in pageFrames
    HT_TableHandle pageTable;
    // increments everytime a page is accessed (used for frame's timeStamp)
    TimeStamp timeStamp;
    // the file handle
    SM_FileHandle pageFile;
   
    
    //statistics
    int numberRead;
    int numberWrite;
    int queuedIndex;
} BM_Metadata;

/* Declaration */
BM_PageFrame *replacementFIFO(BM_BufferPool *const bm);
BM_PageFrame *replacementLRU(BM_BufferPool *const bm);



// use the helper to increase the pool global timestamp & return it
TimeStamp getTimeStamp(BM_Metadata *metadata);
// it helps to evict frame at framedIndex & return new empty frame
BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int framedIndex);

/* Buffer Manager Interface Pool Handling */
// RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
// 		const int numPages, ReplacementStrategy strategy,
// 		void *stratData)
// {
//     // initialize the metadata
//     BM_Metadata *metadata = (BM_Metadata *)malloc(sizeof(BM_Metadata));
//     HT_TableHandle *pageTable = &(metadata->pageTable);
//     metadata->timeStamp = 0;

//     // start the queue from the last element as it gets incremented by one and modded 
//     // at the start of each call of replacementFIFO
//     metadata->queuedIndex = bm->numPages - 1;
//     metadata->numberRead = 0;
//     metadata->numberWrite = 0;
//     RC result = openPageFile((char *)pageFileName, &(metadata->pageFile));

//     switch (result) {
//         case RC_OK:
//             initHashTable(pageTable, PAGE_TABLE_SIZE);
//             metadata->pageFrames = (BM_PageFrame *)malloc(sizeof(BM_PageFrame) * numPages);
//             for (int i = 0; i < numPages; i++)
//             {
//                 metadata->pageFrames[i].framedIndex = i;
//                 metadata->pageFrames[i].data = (char *)malloc(PAGE_SIZE);
//                 metadata->pageFrames[i].fixedCount = 0;
//                 metadata->pageFrames[i].dirty = false;
//                 metadata->pageFrames[i].occupied = false;
//                 metadata->pageFrames[i].timeStamp = getTimeStamp(metadata);
//             }
//             bm->mgmtData = (void *)metadata;
//             bm->numPages = numPages;
//             bm->pageFile = (char *)&(metadata->pageFile);
//             bm->strategy = strategy;
//             return RC_OK;

//         default:
//             // Handle all other cases where the page file cannot be opened
//             bm->mgmtData = NULL;
//             return result;
//     }
// }
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData) 
{
    // Initialize metadata
    BM_Metadata *metadata = (BM_Metadata *)malloc(sizeof(BM_Metadata));
    if (metadata == NULL) {
        return RC_BUFFER_POOL_INIT_FAILED; // Failed to allocate memory for metadata
    }
    HT_TableHandle *pageTable = &(metadata->pageTable);
    metadata->queuedIndex = numPages - 1; // Begin queue from last element
    metadata->numberWrite = 0;
    metadata->timeStamp = 0;
    metadata->numberRead = 0;
   
    // Open the page file
    RC result = openPageFile((char *)pageFileName, &(metadata->pageFile));
if (result != RC_OK) {
    free(metadata);
    bm->mgmtData = NULL;
    return result; // Return the error from openPageFile
}

// Initialize page frames
metadata->pageFrames = (BM_PageFrame *)malloc(sizeof(BM_PageFrame) * numPages);
if (metadata->pageFrames == NULL) {
    free(metadata);
    closePageFile(&(metadata->pageFile));
    bm->mgmtData = NULL;
    return RC_BUFFER_POOL_INIT_FAILED; // Failed to allocate memory for page frames
}

// Initialize hash table
initHashTable(pageTable, PAGE_TABLE_SIZE);

// Initialize each page frame using a do-while loop
int i = 0;
do {
    metadata->pageFrames[i].timeStamp = getTimeStamp(metadata);
    metadata->pageFrames[i].framedIndex = i;
    metadata->pageFrames[i].occupied = false;
    metadata->pageFrames[i].data = (char *)malloc(PAGE_SIZE);
    metadata->pageFrames[i].dirty = false;
    metadata->pageFrames[i].fixedCount = 0;
    i++;
} while (i < numPages);

// Initialize buffer pool fields
bm->pageFile = (char *)&(metadata->pageFile); // Store the page file name
bm->numPages = numPages;
bm->strategy = strategy;
bm->mgmtData = (void *)metadata;

return RC_OK;


RC shutdownBufferPool(BM_BufferPool *const bm) {
    // Make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTable = &(metadata->pageTable);
        
        // It is an error to shutdown a buffer pool that has pinned pages
        for (int i = 0; i < bm->numPages; i++) {
            if (pageFrames[i].fixedCount > 0) {
                return RC_WRITE_FAILED; // Return error if there are pinned pages
            }
        }
        
        forceFlushPool(bm);
        
        // Free each page frame's data using a for loop
        for (int i = 0; i < bm->numPages; i++) {
            free(pageFrames[i].data);
        }

        closePageFile(&(metadata->pageFile));

        // Free the hash table and metadata
        freeHashTable(pageTable);
        free(metadata);
        free(pageFrames);
        
        bm->mgmtData = NULL; // Clear management data pointer
        return RC_OK;
    } else {
        return RC_FILE_HANDLE_NOT_INIT; // Return error if metadata is not initialized
    }
}


RC forceFlushPool(BM_BufferPool *const bm)
{
    // check if the the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        // Initialize loop counter for while loop
        int i = 0; 
        while (i < bm->numPages)
        {
            // write occupied, dirty and unpinned pages to disk
            if (pageFrames[i].occupied && pageFrames[i].dirty && pageFrames[i].fixedCount == 0)
            {
                writeBlock(pageFrames[i].pageNum, &(metadata->pageFile), pageFrames[i].data);
                metadata->numberWrite++;
                pageFrames[i].timeStamp = getTimeStamp(metadata);

                // clear dirty bool
                pageFrames[i].dirty = false;
            }
            // Increment loop counter
            i++; 
        }
        return RC_OK;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

/* Buffer Manager Interface Access Pages */

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    // Check if metadata was successfully initialized
    if (bm->mgmtData != NULL) {
        int framedIndex;
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTable = &(metadata->pageTable);
        
        // Get mapped framedIndex from pageNum
        int getValueResult = getValue(pageTable, page->pageNum, &framedIndex);
        
        // Use a for loop to handle the possible outcomes of getValue
        for (int i = 0; i < 1; i++) { // Loop will run exactly once
            if (getValueResult == 0) {
                pageFrames[framedIndex].timeStamp = getTimeStamp(metadata);

                // Set dirty bool
                pageFrames[framedIndex].dirty = true;
                return RC_OK;
            } else {
                return RC_IM_KEY_NOT_FOUND;
            }
        }
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // see if metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        int framedIndex;
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTable = &(metadata->pageTable);

        // get mapped framedIndex from pageNum
        if (getValue(pageTable, page->pageNum, &framedIndex) == 0)
        {
            pageFrames[framedIndex].timeStamp = getTimeStamp(metadata);

            //force the page if it is not pinned
            if (pageFrames[framedIndex].fixedCount == 0)
            {
                writeBlock(page->pageNum, &(metadata->pageFile), pageFrames[framedIndex].data);
                metadata->numberWrite++;

                // clear dirty bool
                pageFrames[framedIndex].dirty = false;
                return RC_OK;
            }
            else return RC_WRITE_FAILED;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    // Check if management data is initialized
    if (bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;  // Management data not initialized
    }

    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTable = &(metadata->pageTable);
    int framedIndex;

    // Check if the pageNum is valid
    if (pageNum < 0) {
        return RC_IM_KEY_NOT_FOUND;  // pageNum is negative
    }

    // Use a for loop to handle the retrieval and pinning of the page
    for (int i = 0; i < 1; i++) {  // Loop will run exactly once
        int getValueResult = getValue(pageTable, pageNum, &framedIndex);
        
        if (getValueResult == 0) {  // Page is already in a frame
            pageFrames[framedIndex].timeStamp = getTimeStamp(metadata);
            pageFrames[framedIndex].fixedCount++;
            page->pageNum = pageNum;
            page->data = pageFrames[framedIndex].data;
            return RC_OK;
        } else {  // Page is not in a frame, use replacement strategy
            BM_PageFrame *pageFrame;
            switch (bm->strategy) {
                case RS_LRU:
                    pageFrame = replacementLRU(bm);
                    break;
                case RS_FIFO:
                    pageFrame = replacementFIFO(bm);
                    break;
                default:
                    return RC_IM_CONFIG_ERROR;  // Configuration error if no strategy fits
            }

            // Check if replacement strategy succeeded
            if (pageFrame == NULL) {
                return RC_WRITE_FAILED;  // Replacement failed
            }

            // Successful replacement, setup new frame
            setValue(pageTable, pageNum, pageFrame->framedIndex);
            ensureCapacity(pageNum + 1, &(metadata->pageFile));
            readBlock(pageNum, &(metadata->pageFile), pageFrame->data);
            metadata->numberRead++;
            pageFrame->fixedCount = 1;
            pageFrame->occupied = true;
            pageFrame->dirty = false;
            pageFrame->pageNum = pageNum;
            page->pageNum = pageNum;
            page->data = pageFrame->data;
            return RC_OK;
        }
    }

    return RC_OK;  // This line is effectively unreachable
}

/* Statistics Interface */

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    // Use the switch case to check the metadata is initialized or not
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            BM_PageFrame *pageFrames = metadata->pageFrames;

            // Allocate memory for the array; user is responsible for freeing it
            PageNumber *array = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);
            // Initialize loop counter for while loop
            int i = 0;  
            while (i < bm->numPages)
            {
                // Assign page number if frame is occupied, otherwise set to NO_PAGE
                array[i] = pageFrames[i].occupied ? pageFrames[i].pageNum : NO_PAGE;
                i++;  // Increment loop counter
            }
            return array;
        }
        // Return NULL when management data is not initialized
        default:
            return NULL;  
    }
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    // Use switch case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            BM_PageFrame *pageFrames = metadata->pageFrames;

            // Allocate memory for the array; user is responsible for freeing it
            bool *array = (bool *)malloc(sizeof(bool) * bm->numPages);
            // Initialize loop counter for while loop
            int i = 0;  
            while (i < bm->numPages)
            {
                // Set true if the frame is occupied and dirty, otherwise false
                array[i] = pageFrames[i].occupied ? pageFrames[i].dirty : false;
                // Increment loop counter
                i++;  
            }
            return array;
        }
         // Return NULL if management data is not initialized
        default:
            return NULL; 
    }
}

int *getFixCounts (BM_BufferPool *const bm)
{
    // Use the switch case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            BM_PageFrame *pageFrames = metadata->pageFrames;

            // Allocate memory for array user is responsible for freeing it
            int *array = (int *)malloc(sizeof(int) * bm->numPages);
            // Initialize loop counter for while loop
            int i = 0;  
            while (i < bm->numPages)
            {
                // Set fix count if frame is occupied, otherwise set to 0
                array[i] = pageFrames[i].occupied ? pageFrames[i].fixedCount : 0;
                // Increment loop counter
                i++;  
            }
            return array;
        }
            // Return NULL if management data is not initialized
        default:
            return NULL;  
    }
}

int getNumReadIO (BM_BufferPool *const bm)
{
    // Use switch case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            return metadata->numberRead;
        }
        // Return 0 if management data is not initialized
        default:
            return 0;  
    }
}

int getNumWriteIO (BM_BufferPool *const bm)
{
    // Use switch case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            return metadata->numberWrite;
        }
          // Return 0 if management data is not initialized
        default:
            return 0;
    }
}

/* Replacement Policies */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    int firstIndex = metadata->queuedIndex;
    int currentIndex = firstIndex;

    // Keep cycling in FIFO order till a frame is found that is not pinned
    while (true)
    {
        currentIndex = (currentIndex + 1) % bm->numPages;
        if (currentIndex == firstIndex)
            break;
        if (pageFrames[currentIndex].fixedCount == 0)
            break;
    }

    // Update index back into metadata
    metadata->queuedIndex = currentIndex;

    // Check if  all frames are pinned
    switch (pageFrames[currentIndex].fixedCount) 
    {
        case 0:
            return getAfterEviction(bm, currentIndex);
        default:
            return NULL;
    }
}

BM_PageFrame *replacementLRU(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    int minIndex = -1;
    TimeStamp min = UINT_MAX;
    // Initialize loop counter for while loop
    int i = 0;  

    // Find unpinned frame with smallest timestamp
    while (i < bm->numPages)
    {
        if (pageFrames[i].fixedCount == 0 && pageFrames[i].timeStamp < min)
        {
            min = pageFrames[i].timeStamp;
            minIndex = i;
        }
        // Increment loop counter
        i++;  
    }
    
    // Use switch case to handle case where all frames might be pinned
    switch (minIndex)
    {
        // case where all frames were pinned
        case -1:
            return NULL;  
        default:
            return getAfterEviction(bm, minIndex);
    }
}

/* Helpers */

TimeStamp getTimeStamp(BM_Metadata *metadata)
{
    // switch case example that demonstrates the syntax
    switch (metadata != NULL)
    {
        case true:
            return metadata->timeStamp++;
        default:
            // This case should logically never happen if the function is used correctly
            // Returning a default timestamp in case of an error (unexpected)
            return 0;  
    }
}

BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int framedIndex)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Update timestamp
    pageFrames[framedIndex].timeStamp = getTimeStamp(metadata);

    // Use switch-case to handle the occupied status of the page frame
    switch (pageFrames[framedIndex].occupied)
    {
        case true:
            // Remove old mapping
            removePair(pageTable, pageFrames[framedIndex].pageNum);
            // Write old frame back to disk if it's dirty
            switch (pageFrames[framedIndex].dirty)
            {
                case true:
                    writeBlock(pageFrames[framedIndex].pageNum, &(metadata->pageFile), pageFrames[framedIndex].data);
                    metadata->numberWrite++;
                    break;
                default:
                    break;
            }
            break;
        default:
            break;
    }
    // Return the evicted frame (caller must deal with setting the page's metadata)
    return &(pageFrames[framedIndex]);
}
