//////////////////////////////////////////////////////////////////////////
// buf.C
//
// Group 17
// Edison Chiu (ID: 9086051183 )
// Jeff Zhang (ID: 9086143048)
// Ruitao Wu (ID: 9087477130)
//
// Purpose:
// Implements the buffer manager for the project.
// This file contains the buffer manager constructor and destructor,
// buffer frame allocation with the clock replacement algorithm,
// page reading, page unpinning, page allocation, page disposal,
// file flushing, and buffer state printing.
//////////////////////////////////////////////////////////////////////////

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

/*
 * BufMgr
 *
 * Constructor for the buffer manager.
 * Initializes the buffer pool, buffer descriptors, hash table, and clock hand.
 *
 * Input:
 * bufs - number of frames in the buffer pool.
 *
 * Output:
 * None
 *
 * Returns:
 * None
 */
BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}

/*
 * ~BufMgr
 *
 * Destructor for the buffer manager.
 * Flushes any remaining dirty pages in the buffer pool and
 * deallocates memory used by the buffer manager.
 *
 * Input:
 * None.
 *
 * Output:
 * None.
 *
 * Returns:
 * None.
 */
BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/*
 * allocBuf
 *
 * Allocates a buffer frame using the clock replacement algorithm.
 * If necessary, writes a dirty victim page back to disk and removes
 * the old page entry from the hash table before reusing the frame.
 *
 * Input:
 * frame - reference parameter used to return the selected frame number.
 *
 * Output:
 * frame - set to the allocated frame number if successful.
 *
 * Returns:
 * OK - a buffer frame is successfully allocated.
 * BUFFEREXCEEDED - all buffer frames are pinned.
 * UNIXERR - a dirty page cannot be written back to disk.
 * HASHTBLERROR - remove a valid page from the hash table fails.
 */
const Status BufMgr::allocBuf(int & frame) 
{
    for (unsigned int i = 0; i < 2 * (unsigned int)numBufs; i++) 
    {
        advanceClock();
        BufDesc* desc = &bufTable[clockHand];

        // case 1: invalid frame, directly use it
        if (desc->valid == false) 
        {
            frame = clockHand;
            return OK;
        }

        // case 2: recently referenced, give second chance
        if (desc->refbit == true) 
        {
            desc->refbit = false;
            continue;
        }

        // case 3: pinned, cannot replace
        if (desc->pinCnt > 0) 
        {
            continue;
        }

        // case 4: victim found
        if (desc->dirty == true) 
        {
            Status status = desc->file->writePage(desc->pageNo, &bufPool[clockHand]);
            if (status != OK)
                return UNIXERR;
			bufStats.diskwrites++;
        }

		// Remove the old page entry from the hash table
        Status status = hashTable->remove(desc->file, desc->pageNo);
        if (status != OK)
            return HASHTBLERROR;

		// Remove the old page entry from the hash table
        desc->Clear();
        frame = clockHand;
        return OK;
    }

    // All frames are pinned
    return BUFFEREXCEEDED;
}

/*
 * readPage
 *
 * Looks up the requested page in the buffer pool.
 * If the page is already present, updates its refbit and pin count.
 * Otherwise, allocates a buffer frame, reads the page from disk,
 * inserts the page into the hash table, and sets the buffer descriptor.
 *
 * Input:
 * file - pointer to the file containing the requested page.
 * PageNo - page number to read.
 *
 * Output:
 * page - set to point to the requested page in the buffer pool.
 *
 * Returns:
 * OK - the page is successfully found or loaded.
 * UNIXERR - reading the page from disk fails (Unix error).
 * BUFFEREXCEEDED - no buffer frame can be allocated.
 * HASHTBLERROR - a hash table error occurred.
 */	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    Status status;
    int frameNo;
	bufStats.accesses++;

    status = hashTable->lookup(file, PageNo, frameNo);

    // Case 1: page already in buffer pool
    if (status == OK)
    {
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }

    // Case 2: page not in buffer pool
    if (status == HASHNOTFOUND)
    {
        status = allocBuf(frameNo);
        if (status != OK)
            return status;

        // Read the page from disk into the buffer pool frame
        status = file->readPage(PageNo, &bufPool[frameNo]);
        if (status != OK)
            return UNIXERR;

		bufStats.diskreads++;

        // Insert the page into the hash table
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK)
            return HASHTBLERROR;

		// Set  buffer descriptor for the newly loaded page
        bufTable[frameNo].Set(file, PageNo);
        page = &bufPool[frameNo];
        return OK;
    }

    return HASHTBLERROR;
}

/*
 * unPinPage
 *
 * Unpins the specified page in the buffer pool by decrementing its
 * pin count. If the dirty flag is true, the page is marked dirty.
 *
 * Input:
 * file - pointer to the file containing the page.
 * PageNo - page number to unpin.
 * dirty - true if the page has been modified, false otherwise.
 *
 * Output:
 * None.
 *
 * Returns:
 * OK - the page is successfully unpinned.
 * HASHNOTFOUND - the page is not found in the buffer pool hash table.
 * PAGENOTPINNED - the page pin count is already 0.
 * HASHTBLERROR - a hash table error occurs.
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

	// Page not found in hash table
	if (status == HASHNOTFOUND)
		return HASHNOTFOUND;

	// Unexpected hash table error
	if (status != OK)
		return HASHNOTFOUND;

	// Page is already unpinned
    if (bufTable[frameNo].pinCnt == 0)
        return PAGENOTPINNED;

	// Decrease pin count by 1
    bufTable[frameNo].pinCnt--;

	// Mark the page dirty if modified
    if (dirty)
        bufTable[frameNo].dirty = true;

    return OK;
}

/*
 * allocPage
 *
 * Allocates a new empty page in the given file and assigns a buffer
 * frame to hold it. Initializes the page in the buffer pool, inserts
 * it into the hash table, and sets the buffer descriptor.
 *
 * Input:
 * file - pointer to the file in which the new page is allocated.
 *
 * Output:
 * pageNo - set to the newly allocated page number.
 * page   - set to point to the new page in the buffer pool.
 *
 * Returns:
 * OK - the page and buffer frame are successfully allocated.
 * UNIXERR - allocating the page in the file fails (Unix error).
 * BUFFEREXCEEDED - no buffer frame can be allocated.
 * HASHTBLERROR - hash table error occurred.
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status;
    int frameNo;
	bufStats.accesses++;

    // Allocate an empty page in the file
    status = file->allocatePage(pageNo);
    if (status != OK)
	{
        return UNIXERR;
	}

	bufStats.diskreads++;

    // Allocate a buffer pool frame
    status = allocBuf(frameNo);
    if (status != OK)
	{
		file->disposePage(pageNo);
		return status;
	}

	bufPool[frameNo].init(pageNo);

    // Insert into hash table
    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK)
    {
		file->disposePage(pageNo);
		return HASHTBLERROR;
	}

    // Set up the frame
    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];

    return OK;
}

/*
 * disposePage
 *
 * Removes the specified page from the buffer pool if it is present
 * and deallocates the page from the file.
 *
 * Input:
 * file - pointer to the file containing the page.
 * pageNo - page number to dispose.
 *
 * Output:
 * None.
 *
 * Returns:
 * The status returned by the file layer when disposing the page.
 */
const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}


/*
 * flushFile
 *
 * Flushes all pages of the specified file from the buffer pool.
 * Dirty pages are written back to disk, their hash table entries
 * are removed, and their buffer descriptors are cleared.
 *
 * Input:
 * file - pointer to the file whose pages should be flushed.
 *
 * Output:
 * None.
 *
 * Returns:
 * OK - all pages are successfully flushed.
 * PAGEPINNED - any page of the file is still pinned.
 * BADBUFFER - an invalid buffer state is detected.
 */
const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}

/*
 * printSelf
 *
 * Prints the current contents and status of the buffer pool.
 * This function is intended for debugging and inspection.
 *
 * Input:
 * None.
 *
 * Output:
 * Writes buffer state information to standard output.
 *
 * Returns:
 * None.
 */
void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


