/* Storage Manager responsible for reading and writing of memory to/from the file.*/

//Input/Output header file
#include <stdio.h>
//Included for malloc and calloc
#include <stdlib.h>
//Inclusion of Helper files
#include "storage_mgr.h"

//Global file pointer
FILE *file;

//File Intialization
void initStorageManager(void){
    file= NULL;
}

//Creation of new file having filename passed as argument
RC createPageFile(char *fileName){

    FILE *file=fopen(fileName,"w");
    if(file == NULL){
        return RC_FILE_CREATION_FAILED; //Returning file creation error
    }
    fseek(file,PAGE_SIZE,SEEK_SET);
	fputc('\0',file);
    fclose(file);
    return RC_OK;
}

//Opening of Existing file and if file does not exist throws error
RC openPageFile(char *fileName, SM_FileHandle *fHandle){
    FILE *file;
    file= fopen(fileName,"r");
    if(file == NULL){
        return RC_FILE_NOT_FOUND; //Returning file not found error
    }
    fHandle-> fileName = fileName;
    fHandle-> curPagePos = 0;
    fseek(file,0,SEEK_END);
    fHandle->totalNumPages= ((ftell(file))/PAGE_SIZE);
    fHandle->mgmtInfo=file; //updating file details
    fclose(file);
    return RC_OK;   
}

//Closing file and releasing memory
RC closePageFile(SM_FileHandle *fHandle){
    FILE *fp= fopen(fHandle->fileName,"r");
    if(fseek(fp,0,SEEK_SET)==0){
        fclose(fp);
        fp=NULL;
        return RC_OK;
    }
    return RC_FILE_NOT_FOUND; //Returning file not found error
}

//Removes file
RC destroyPageFile(char * fileName){
    if(remove(fileName) == 0){
        return RC_OK;
    }
    return RC_FILE_NOT_FOUND; //Returning file not found error
}

//Reads a block of data from the pageNum passed as argument and write it to the memPage
RC readBlock(int pageNum,SM_FileHandle *fHandle,SM_PageHandle memPage){
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    else if(pageNum<0 || pageNum > fHandle->totalNumPages){
        return RC_READ_NON_EXISTING_PAGE;
    }
    else{
        FILE *fp = fopen(fHandle->fileName,"r");
        if(fp==NULL){
            fclose(fp);
            return RC_FILE_NOT_FOUND; //Returning file not found error
        }
        else{
            fseek(fp,(pageNum)*PAGE_SIZE,SEEK_SET);
            fread(memPage,PAGE_SIZE,sizeof(char),fp);
            fHandle->curPagePos=ftell(fp);
            fclose(fp);
            return RC_OK;
        }
    }
}

//Returns Current file pointer position
RC getBlockPos(SM_FileHandle *fHandle){
    return fHandle->curPagePos;
}

//Reads block from current page pointer 
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos/PAGE_SIZE,fHandle,memPage);
}

//Reads first block of data
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0,fHandle,memPage);
}

//Reads previous block of data from current file pointer
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos-1,fHandle,memPage);
}

//Reads next block of data from current file pointer
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos+1,fHandle,memPage);
}

//Reads last block of data 
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->totalNumPages-1,fHandle,memPage);
}

//Writes block of data
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;  //Returning file intialization error
    }
    else if(pageNum<0 || pageNum > fHandle->totalNumPages){
        return RC_WRITE_FAILED;  //Returning file not found error
    }
    else{
       FILE *fp = fopen(fHandle->fileName,"r+");
        if(fp==NULL){
            fclose(fp);
            return RC_FILE_NOT_FOUND; //Returning file not found error
        }
        else{
            fseek(fp,pageNum*PAGE_SIZE,SEEK_SET);
            fwrite(memPage,sizeof(char),PAGE_SIZE,fp);  //Writing block of data from pagnum argument
            fHandle->curPagePos=ftell(fp);
            fclose(fp);
            return RC_OK;
        } 
    }
}

//writes file block pointed by current file pointer
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return writeBlock(fHandle->curPagePos,fHandle,memPage);
}

//Appending the file with an empty block at the end of file
RC appendEmptyBlock(SM_FileHandle *fHandle){
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;  //
    }
    else{
        FILE *file= fopen(fHandle->fileName,"a");
        fseek(file,(fHandle->totalNumPages+1)*PAGE_SIZE,SEEK_END);
        char *ptr= (char *)malloc(PAGE_SIZE);
        fwrite(ptr,sizeof(char),PAGE_SIZE,file);  //Appending single Block of data to file
        fclose(file);
        return RC_OK;
    }
}

//Checking number of pages with current file size and if not then it creates new pages in a file upto numberOfPages and sets totalNumPages to numberOfPages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle){
    if(fHandle==NULL){
        return RC_FILE_NOT_FOUND; //Returning file not found error
    }
    else{
        if(numberOfPages>fHandle->totalNumPages){
            int extraBlocks= numberOfPages-fHandle->totalNumPages;
            while(extraBlocks>0){
                appendEmptyBlock(fHandle);  //Adding single block at a time
                extraBlocks--;
            }
        }
        else{
            return RC_OK;
        }
    }
}