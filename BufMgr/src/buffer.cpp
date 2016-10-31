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
}

void BufMgr::advanceClock()
{
    //std::cout << "advancing";

    if(clockHand == numBufs - 1)
    {
        clockHand = 0;
    }

    else
    {
        clockHand++;
    }
}

void BufMgr::allocBuf(FrameId & frame) 
{
    //if we go all the way around, throw error
    FrameId currentFrameNo = clockHand;

    //tell if unallocated frame
    int found = 0;
    int first = 0;
    int noPinned = 1;

    //while not found
    while(found == 0)
    {
        if(found == 0 && first != 0 && noPinned == 1 && clockHand == currentFrameNo)
        {
            std::cout << "everything is pinned\n";

            exit(0);
        }

        //advance clock
        advanceClock();

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
                noPinned = false;
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
                     //we choose this frame to replace
                     frame = clockHand;
                     found = 1;
                 }
             }

        }

        first = 1;

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
        FrameId newFrameIdx;
      
        //get index of new buffer frame
        allocBuf(newFrameIdx); 

       if(bufDescTable[newFrameIdx].valid == false)
       {

            //get page
            Page getPage = file -> readPage(pageNo);

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

        else
        {
            //get current frame
            BufDesc currentFrame = bufDescTable[newFrameIdx];

            //if dirty, write
            if(currentFrame.dirty == true)
            {
                //flush file
                currentFrame.file -> writePage(bufPool[newFrameIdx]);
            }

            hashTable -> remove(currentFrame.file, currentFrame.pageNo);

            //get page
            Page getPage = file -> readPage(pageNo);

            //insert page into hash table
            hashTable -> insert(file, pageNo, newFrameIdx);

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
        std::cout << "PNP error: page isn't pinned\n";
        exit(0);
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
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    //allocate page
    Page emptyPage = file -> allocatePage();

    //get corresponding page number
    pageNo = emptyPage.page_number();

    FrameId getFrame;
    allocBuf(getFrame);

    //not replacing
    if(bufDescTable[getFrame].valid != true)
    {

        hashTable -> insert(file, pageNo, getFrame);
        BufDesc currentFrame = bufDescTable[getFrame];
        currentFrame.Set(file, pageNo);

        //update frame information
        bufDescTable[getFrame] = currentFrame;

        //update page info
        bufPool[getFrame] = emptyPage;
        page = &(bufPool[getFrame]);
    }

    //replacing
    else
    {
        //remove it
        BufDesc replaceFrame = bufDescTable[getFrame];

        //if replace frame has dirty bit set
        if(replaceFrame.dirty == true)
        {
            //flush file
            replaceFrame.file -> writePage(bufPool[getFrame]);
        }

        hashTable -> remove(replaceFrame.file, replaceFrame.pageNo);

        hashTable -> insert(file, pageNo, getFrame);

        replaceFrame.Set(file, pageNo);

        //update frame information
        bufDescTable[getFrame] = replaceFrame;

        //update page Info
        bufPool[getFrame] = emptyPage;
        page = &(bufPool[getFrame]);
    }
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
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
