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


const Status BufMgr::allocBuf(int & frame)
{
    // Clock algorithm: scan up to 2*numBufs times to find a free frame
    for (unsigned int i = 0; i < (unsigned int)numBufs * 2; i++) {
        advanceClock();
        BufDesc* buf = &bufTable[clockHand];

        // If frame is not valid, it's free to use
        if (!buf->valid) {
            frame = clockHand;
            return OK;
        }

        // If refbit is set, clear it and move on
        if (buf->refbit) {
            buf->refbit = false;
            continue;
        }

        // If page is pinned, skip it
        if (buf->pinCnt > 0) {
            continue;
        }

        // Found an unpinned frame with refbit == false
        // If dirty, flush to disk first
        if (buf->dirty) {
            Status status = buf->file->writePage(buf->pageNo, &bufPool[clockHand]);
            if (status != OK)
                return UNIXERR;
            bufStats.diskwrites++;
        }

        // Remove from hash table
        hashTable->remove(buf->file, buf->pageNo);

        // Clear the frame
        buf->Clear();
        frame = clockHand;
        return OK;
    }

    // All frames are pinned
    return BUFFEREXCEEDED;
}


const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status == HASHNOTFOUND) {
        // Case 1: Page is not in the buffer pool
        // Allocate a buffer frame
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

        // Set up the frame
        bufTable[frameNo].Set(file, PageNo);
        page = &bufPool[frameNo];
    } else {
        // Case 2: Page is in the buffer pool
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
    }

    bufStats.accesses++;
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo,
			       const bool dirty)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK)
        return HASHNOTFOUND;

    if (bufTable[frameNo].pinCnt == 0)
        return PAGENOTPINNED;

    bufTable[frameNo].pinCnt--;

    if (dirty)
        bufTable[frameNo].dirty = true;

    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)
{
    // Allocate an empty page in the file
    Status status = file->allocatePage(pageNo);
    if (status != OK)
        return UNIXERR;

    // Allocate a buffer pool frame
    int frameNo;
    status = allocBuf(frameNo);
    if (status != OK)
        return status;

    // Insert into hash table
    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK)
        return HASHTBLERROR;

    // Set up the frame
    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];

    bufStats.accesses++;
    return OK;
}

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
