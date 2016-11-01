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


BufMgr::~BufMgr() 
{
    for (std::uint32_t i = 0; i < numBufs; i++)
    {
        BufDesc* getBuffer = &(bufDescTable[i]);

        //if dirty, write back
        if(getBuffer -> dirty == true && getBuffer -> valid == true)
        {
            //flush file
            getBuffer -> file -> writePage(bufPool[i]);
            bufStats.diskwrites++;
        }
    }

    delete hashTable;
    delete [] bufPool;
    delete [] bufDescTable;
}

void BufMgr::advanceClock()
{
    //advance clock
    //if goes out of bounds, cycle back
    clockHand++;
    clockHand = clockHand % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
    //if we go all the way around, throw error
    FrameId currentFrameNo = clockHand;

    //tell if unallocated frame
    int found = 0;
    int first = 1;
    int allPinned = 1;

    //while not found
    while(found == 0)
    {
        //if not found, all frames are pinned, not the first iteration, and 
        //we did a full loop, then the buffer is full
        if(clockHand == currentFrameNo)
        {
            if(found == 0)
            {
                if(first != 1)
                {
                    if(allPinned == 1)
                    {
                        throw BufferExceededException();
                    }
                }  
            }
        }

        //advance clock
        advanceClock();
        first = 0;

        //get frame
        BufDesc getFrame = bufDescTable[clockHand];

        //if frame is not valid, choose it
        if(getFrame.valid == false)
        {
            frame = clockHand;
            found = 1;
        }

        //frame is valid
        else
        {
            //check pinning
            if(getFrame.pinCnt == 0)
            {
                //there will be some frame to replace
                allPinned = 0;
            }

            //check ref bit
            if(getFrame.refbit == true)
            {
                 //set refbit
                 getFrame.refbit = false;

                 //update frame
                 bufDescTable[clockHand] = getFrame;

                 //continue to advance clock
                 continue;
             }

             else
             {
                 //if pinned, continue
                 if(getFrame.pinCnt > 0)
                 {
                     continue;
                 }

                 else
                 {
                     //if dirty, write
                     if(getFrame.dirty == true)
                     {
                         //flush file
                         getFrame.file -> writePage(bufPool[clockHand]);
                         bufStats.diskwrites++;
                     }

                     hashTable -> remove(getFrame.file, getFrame.pageNo);

                     //we choose this frame to evict
                     frame = clockHand;
                     found = 1;
                 }
             }
        }

    }
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    //in buffer pool
    try
    {
        FrameId getFrameIdx;

        //look up the index
        hashTable -> lookup(file, pageNo, getFrameIdx);
    
        //get the frame
        BufDesc getFrame = bufDescTable[getFrameIdx];
  
        //set ref bit
        getFrame.refbit = true;

        //increment pin count
        getFrame.pinCnt = (getFrame.pinCnt + 1);

        //return pointer to page
        page = &(bufPool[getFrameIdx]);

        //update frame
        bufDescTable[getFrameIdx] = getFrame;
    }

    //not in buffer pool
    catch(HashNotFoundException &e)
    {
        //get page
        Page getPage = file -> readPage(pageNo);
        bufStats.diskreads++;
 
        FrameId newFrameIdx;
      
        //get index of new buffer frame
        allocBuf(newFrameIdx); 

        //insert page into hash table
        hashTable -> insert(file, pageNo, newFrameIdx);

        //get current frame
        BufDesc currentFrame = bufDescTable[newFrameIdx];
       
        //set frame
        currentFrame.Set(file, pageNo);

        //update frame
        bufDescTable[newFrameIdx] = currentFrame;

        //put page into pool
        bufPool[newFrameIdx] = getPage;
        
        //return
        page = &(bufPool[newFrameIdx]);

    }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    //this is the frame id
    FrameId getFrameIdx;

    //look up where the frame is
    hashTable -> lookup(file, pageNo, getFrameIdx);

    //get that frame
    BufDesc getFrame = bufDescTable[getFrameIdx];

    if(getFrame.pinCnt == 0)
    {
        throw PageNotPinnedException(file -> filename(), pageNo, getFrameIdx);
    }

    //decrement pin
    getFrame.pinCnt = (getFrame.pinCnt - 1);
  
    if(dirty == true)
    {
        getFrame.dirty = true;
    }

    //update
    bufDescTable[getFrameIdx] = getFrame;
}

void BufMgr::flushFile(const File* file) 
{
    for (std::uint32_t i = 0; i < numBufs; i++)
    {
        BufDesc* getBuffer = &(bufDescTable[i]);

        //if files are equal
        if(getBuffer -> file == file)
        {
            //invalid frame, throw exception
            if(getBuffer -> valid == false)
            {
                 throw BadBufferException(i, getBuffer -> dirty, getBuffer -> valid, getBuffer -> refbit);
            }

            //if pinned throw exception
            if(getBuffer -> pinCnt > 0)
            {
                throw PagePinnedException(file -> filename(), bufPool[i].page_number(), i);
            }

            //if dirty, write back
            if(getBuffer -> dirty)
            {
                //flush file
                getBuffer -> file -> writePage(bufPool[i]);
                bufStats.diskwrites++;
            }

            //remove from table
            hashTable -> remove(getBuffer -> file, getBuffer -> pageNo);

            //clear
            getBuffer -> Clear();

            //update
            bufDescTable[i] = *(getBuffer);
        }
    }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    //allocate page
    Page emptyPage = file -> allocatePage();

    //get corresponding page number
    pageNo = emptyPage.page_number();

    FrameId getFrame;
    allocBuf(getFrame);

    hashTable -> insert(file, pageNo, getFrame);
    BufDesc currentFrame = bufDescTable[getFrame];
    currentFrame.Set(file, pageNo);

    //update frame information
    bufDescTable[getFrame] = currentFrame;

    //update page info
    bufPool[getFrame] = emptyPage;
    page = &(bufPool[getFrame]);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    //try to removepage from buffer
    try
    {
        //this is the frame id
        FrameId getFrameIdx;

        //look up where the frame is
        hashTable -> lookup(file, PageNo, getFrameIdx);

        //getFrame
        BufDesc getFrame = bufDescTable[getFrameIdx];

        //if page is pinned, throw exception
        if(getFrame.pinCnt > 0)
        {
            throw PagePinnedException(file -> filename(), PageNo, getFrameIdx);
        }

        else
        {
            //free frame
            getFrame.Clear();

            //update Frame
            bufDescTable[getFrameIdx] = getFrame;

            //remove from hash table
            hashTable -> remove(file, PageNo);
        }
    }
 
    catch(HashNotFoundException &e)
    {
        
    }   

    //remove page from file
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
