/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

  BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs) {
    bufDescTable = new BufDesc[bufs];

    for (FrameId i = 0; i < bufs; i++) 
      {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
      }

    bufPool = new Page[bufs];

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
  }


  BufMgr::~BufMgr() {
    for(FrameId i = 0; i < numBufs; i++) {
      if(bufDescTable[i].dirty) {
	// flush file containing page
	flushFile(bufDescTable[i].file);
      }
    }

    delete[] bufDescTable;
    delete hashTable;
  }

  void BufMgr::advanceClock()
  {
    clockHand++;
    clockHand = clockHand % numBufs;
  }

  void BufMgr::allocBuf(FrameId & frame) 
  {
    for(uint32_t i = 0; i < 2*numBufs; i++) {
      advanceClock();

      //if the frame is not valid
      if(!(bufDescTable[clockHand].valid)) {
	//return
	frame = clockHand;
	return;
      }
          
      // clear refbit
      if(bufDescTable[clockHand].refbit) {
	bufDescTable[clockHand].refbit = false;
	continue;
      }
      else {
	// if not pinned
	if(bufDescTable[clockHand].pinCnt == 0) {
	  // if dirty
	  if(bufDescTable[clockHand].dirty) {
            //write page back
	    bufDescTable[clockHand].file -> writePage(bufPool[clockHand]);
	  }
	  //remove from hash table
	  try {
	    hashTable -> remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
	  } catch(HashNotFoundException e) {
	    //do nothing
	  }
          //return frame
	  frame = clockHand;
	  return;
	}
      }
    }
    //too many allocations	  
    throw BufferExceededException();
  }

	
  void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
  {
    // frame id
    FrameId frame;
    try {
      // get frame id
      hashTable->lookup(file, pageNo, frame);
      bufDescTable[frame].refbit = true;
      bufDescTable[frame].pinCnt++;
    } catch(HashNotFoundException e) {
      // allocate new frame
      allocBuf(frame);
      
      // read page from file
      // @throws  InvalidPageException  If the page is free (unused) and
      //                                allow_free is false.
      bufPool[frame] = file -> readPage(pageNo);

      // update pool
      // @throws HashAlreadyPresentException
      // @throws HashTableException (optional) if could not create a new bucket as running of memory
      hashTable -> insert(file, pageNo, frame);

      bufDescTable[frame].Set(file, pageNo);
      
    }
    // return pointer to the page
    page = &bufPool[frame];
  }


  void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
  {
    // frame id
    FrameId frame;
    try {
      // get frame id
      hashTable -> lookup(file, pageNo, frame);

      if(bufDescTable[frame].pinCnt == 0) {
	throw PageNotPinnedException(file -> filename(), pageNo, frame);
      }
      
      bufDescTable[frame].pinCnt--;
      if (dirty) {
	bufDescTable[frame].dirty = dirty;
      }
    } catch(HashNotFoundException e) {
      // do nothing
    }
  }

  void BufMgr::flushFile(const File* file) 
  {
    // pointer to page in buffer pool
    BufDesc* page;    
    for(FrameId i = 0; i < numBufs; i++) {
      page = &bufDescTable[i];
      // if page belongs to the file
      if(page -> file == file) {
	// if not valid
	if(!(page -> valid)) {
	  throw BadBufferException(page -> frameNo, page -> dirty, page ->  valid, page -> refbit);
	}
	// if pinned
	if(page -> pinCnt != 0) {
	  throw PagePinnedException(file -> filename(), page -> pageNo, page -> frameNo);
	}
	// if dirty
	if(page -> dirty) {
	  // write to disk
	  page -> file -> writePage(bufPool[i]);
	  // remove from hash table
	  // @throws HashNotFoundException
	  try {
	    hashTable -> remove(file, page -> pageNo);
	  } catch(HashNotFoundException e) {
	    // do nothing
	  }
	  bufDescTable[i].Clear(); 
	}
      }
    }
  }

  void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
  {
    // allocate empty page
    Page newpage = file -> allocatePage();
    pageNo = newpage.page_number();

    // insert into frame in the buffer pool
    FrameId frame;
    allocBuf(frame);
    bufPool[frame] = newpage;
    bufDescTable[frame].Set(file, pageNo);
    
    // insert to hash table
    // @throws HashAlreadyPresentException if the corresponding page already exists in the hash table
    // @throws HashTableException (optional) if could not create a new bucket as running of memory
    hashTable -> insert(file, pageNo, frame);

    page = &bufPool[frame];
  }

  void BufMgr::disposePage(File* file, const PageId PageNo)
  {
    // identify frame
    FrameId frame;
    try {
      hashTable -> lookup(file, PageNo, frame);

      // remove from buffer pool
      bufDescTable[frame].Clear();

      // remove from hash table
      hashTable -> remove(file, PageNo);
    } catch(HashNotFoundException e) {
      // do nothing
    }
    // delete page from file
    file -> deletePage(PageNo);
  }

  void BufMgr::printSelf(void) 
  {
    BufDesc* tmpbuf;
    int validFrames = 0;
  
    for (std::uint32_t i = 0; i < numBufs; i++)
      {
  	tmpbuf = &(bufDescTable[i]);
	std::cout << "FrameNo:" << i << " ";
	tmpbuf->Print();

  	if (tmpbuf->valid == true)
	  validFrames++;
      }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
  }

}
