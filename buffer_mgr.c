#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"


#define RC_ERROR 404
#define RC_DIRTY_PAGES 500

typedef struct BufferManager {
	SM_PageHandle data; 
	PageNumber pageNum;
	int dirtyFlag;
	int fixCount;
	int lrPage; 
	int LRpage;
} BufferManager;

int lastBuffer,pageUnpin = 0;
int pageLoc ,bufferSize =0;
int totalWriteIO ,activePage ,leastUsedAdd,pagePos = 0;

RC forceFlushPool(BM_BufferPool *const bufferPool) {
	BufferManager *bufferm = (BufferManager *)bufferPool->mgmtData;

	int j = 0;
	while (j < bufferSize) 
	{
		SM_FileHandle fh;
		if (bufferm[j].fixCount == 0 && bufferm[j].dirtyFlag == 1) {
			//Opening page file
			openPageFile(bufferPool->pageFile, &fh);
			bufferm[j].dirtyFlag = 0;
			//Writing it to back to disk
			writeBlock(bufferm[j].pageNum, &fh, bufferm[j].data);
			
			totalWriteIO++;
		}
		j++;
	}
	
	return RC_OK;
}

RC initBufferPool(BM_BufferPool *const bm, 
const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) 
{
	bm->pageFile = (char *)pageFileName;
	bm->strategy = strategy;

	bm->numPages = numPages;
	bufferSize = numPages;

	BufferManager *bufferm = malloc(sizeof(BufferManager) * numPages);	

	int i=0;
	while(i<bufferSize)
	{
		bufferm[i].data = NULL;
		bufferm[i].pageNum = -1;
		bufferm[i].dirtyFlag = bufferm[i].lrPage = bufferm[i].LRpage = bufferm[i].fixCount = 0;
		i++;
	}
	totalWriteIO = leastUsedAdd = activePage = 0;
	bm->mgmtData = bufferm;

	return RC_OK;	
}

// Shutting down bufferpool
RC shutdownBufferPool(BM_BufferPool *const bufferMgr) {
	forceFlushPool(bufferMgr);
	BufferManager *bm = (BufferManager *)bufferMgr->mgmtData;
	int i=0;
	while ( i< bufferSize) {
		if (bm[i].fixCount != 0) {
			return RC_DIRTY_PAGES;
		}
		i++;
	}

	free(bm);
	bufferMgr->mgmtData = NULL;

	return RC_OK;
}

/************************************************************/
/*            Buffer Manager Interface Access Pages          /
/************************************************************/

RC markDirty(BM_BufferPool *const bufferPool, BM_PageHandle *const bufferPage) {
	BufferManager *bm = (BufferManager *)bufferPool->mgmtData;
	int i=0;
	int n;
  	double b[10];

  	for (int i = 0; i < bm[1].pageNum; i++) {
	    b[i]= (i+2)%n;
	}

	while(i<bufferSize)
	{
		if (bm[i].pageNum == bufferPage->pageNum) 
		{
			bm[i].dirtyFlag = 1;

			return RC_OK;		
		}		
		i++;	
	}		
	return RC_ERROR;
}

RC unpinPage(BM_BufferPool *const bufferPool, BM_PageHandle *const bufferPage) {	
	BufferManager *bm = (BufferManager *)bufferPool->mgmtData;
	int unpinPage=bufferPage->pageNum;
	pageUnpin++;
	int j=0;
	for(int j=0;j<bufferSize;j++)
	{
		if (bm[j].pageNum == unpinPage) {
			bm[j].fixCount--;

			return RC_OK;		
		}
	}
	return RC_OK;
}

RC forcePage(BM_BufferPool *const bufferPool, BM_PageHandle *const bufferPage) {
	BufferManager *bm = (BufferManager *)bufferPool->mgmtData;
	pageUnpin++;
	int i=0;
	
	while(i<bufferSize)
	{
		SM_FileHandle fh;
		if (bm[i].pageNum == bufferPage->pageNum)
		 {	
			//Opening and Write page back to disk	
			openPageFile(bufferPool->pageFile, &fh);
			bm[i].dirtyFlag = 0;

			writeBlock(bm[i].pageNum, &fh, bm[i].data);
			totalWriteIO++;
		}
		i++;
	}	
	return RC_OK;
}



RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
	BufferManager *bufferManager = (BufferManager *)bm->mgmtData;
	int noPage= -1;
	int totalPageRead=10;
	int i=0;
	bool isBufferFull = true;
	int reverse=0;
	int rem;
	
	if (bufferManager[0].pageNum == -1) {
		SM_FileHandle fh;
		while(totalPageRead<noPage)    
		{    
			rem=totalPageRead%10;      
		} 
		//Opening a file
		openPageFile(bm->pageFile, &fh);
		//Checking Capacity
		pageUnpin++;
		ensureCapacity(pageNum,&fh);
		rem=reverse*totalPageRead;
		bufferManager[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		//Writing back to disk
		readBlock(pageNum, &fh, bufferManager[0].data);
		rem=bufferManager[0].pageNum;
		bufferManager[0].pageNum = pageNum;
		reverse++;
		bufferManager[0].fixCount++;
		bufferManager[0].LRpage = 0;
		rem=bufferManager[0].LRpage;
		
		pageLoc = 0;
		reverse=rem=pageLoc;
		bufferManager[0].lrPage = pageLoc;	
		
		lastBuffer = 0;
		reverse++;
		page->pageNum = pageNum;
		page->data = bufferManager[0].data;
		
		return RC_OK;		
	} else {	
		while (i < bufferSize) {
			SM_FileHandle fh;
			int n=10;
			double b[totalPageRead];

			for (int i = 0; i < n; i++) {
				b[i]= (i+2)%n;
			}
			if (bufferManager[i].pageNum == -1) {	
				openPageFile(bm->pageFile, &fh);
				b[i%n]++;
				bufferManager[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, bufferManager[i].data);

				b[i%n]= bufferManager[i].pageNum;
				bufferManager[i].pageNum = pageNum;
				bufferManager[i].fixCount = 1;
				b[(i+1)%n]=bufferManager[i].LRpage;
				bufferManager[i].LRpage = 0;
				lastBuffer++;	
				pageLoc++;
				 

				if (bm->strategy == RS_LRU){
					b[i%n]= pageLoc;
					bufferManager[i].lrPage = pageLoc;				
				}else if (bm->strategy == RS_CLOCK){
					b[i%n]= 1;
					bufferManager[i].lrPage = 1;
				}
						
				page->pageNum = pageNum;
				pageUnpin++;
				page->data = bufferManager[i].data;
				
				return RC_OK;
							
			} else if (bufferManager[i].pageNum == pageNum) {
				pageUnpin++;
				pageLoc++; 
				if (bm->strategy == RS_LRU) {
					b[i%n]=pageLoc;
					bufferManager[i].lrPage = pageLoc;
				} else if (bm->strategy == RS_CLOCK) {
					b[(i+1)%n]=1;
					bufferManager[i].lrPage = 1;
				} else if(bm->strategy == RS_LFU) {
					b[i%n]++;
					bufferManager[i].LRpage++;
				}
				page->pageNum = pageNum;
				b[i%n]= pageNum;
				page->data = bufferManager[i].data;
				
				bufferManager[i].fixCount++;
				activePage++;

				return RC_OK;	
			}
			i++;
		}
	}		
	return RC_OK;
}


/************************************************************
*              STATISTICS FUNCTIONS                       *
************************************************************/

PageNumber *getFrameContents(BM_BufferPool *const bm) {
	BufferManager *bufferManager = (BufferManager *) bm->mgmtData;
	int i=0;
	PageNumber *pageF = malloc(sizeof(PageNumber) * bufferSize);
	
	while(i < bufferSize)
	{
		if (bufferManager[i].pageNum != -1) {
			pageF[i] = bufferManager[i].pageNum;
		} else {
			pageF[i] = -1;
		}
		i++;
	}
	
	return pageF;
}

bool *getDirtyFlags (BM_BufferPool *const bm) {
	bool *dirtyFlags = malloc(sizeof(bool) * bufferSize);
	int i=0;
	BufferManager *bufferManager = (BufferManager *)bm->mgmtData;
	
	while(i < bufferSize)
	{
		if (bufferManager[i].dirtyFlag == 1) {
			dirtyFlags[i] = true;
		} else {
			dirtyFlags[i] = false;
		}
		i++;
	}	

	return dirtyFlags;
}

int *getFixCounts (BM_BufferPool *const bm) {
	int *fixcount = malloc(sizeof(int) * bufferSize);
	int i=0;
	BufferManager *bufferM= (BufferManager *)bm->mgmtData;
	
	while(i < bufferSize)
	{
		if (bufferM[i].fixCount != -1) {
			fixcount[i] = bufferM[i].fixCount;
		} else {
			fixcount[i] = 0;
		}
		i++;
	}

	return fixcount;
}

int getNumReadIO (BM_BufferPool *const bm) {
	return lastBuffer + 1;
}

int getNumWriteIO (BM_BufferPool *const bm) {
	return totalWriteIO;
}