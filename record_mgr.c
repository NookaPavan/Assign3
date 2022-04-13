#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

typedef struct Table {
	RID ID;
	Expr *expr;
	int totalRows; // total number of rows in the table
	int freePage;    // first free page
	int totalReads;   // number of records scanned
	BM_BufferPool bufferPool;   // bufferpool	
	BM_PageHandle bufferHandle;	// Buffer Manager PageHandle 	
} Table;

Table *table;

/************************************************************
*         Table and Record Manager Functions.               *
************************************************************/
int attributeLength = 20;

// Initiliazing Storage Manager
RC initRecordManager(void *mgmtData) {
	initStorageManager();
	return RC_OK;
}

// shuts down the Record Manager
RC shutdownRecordManager() {
	printf("Shutting down Record Manager \n");
	free(table);
	return RC_OK;
}
long pageAdd = sizeof(int);

RC createContent(char *data, Schema *schema) {
	int n=attributeLength;
	int i = 0;
	char *content = data;
	int totalAttr = schema->numAttr;
	content = content + pageAdd;
	int reslt=0;
	int ptr;

	*(int*)content = 1; 
	content = content + pageAdd; 

	*(int*)content = schema->numAttr; // Setting the number of attributes
	content = content + pageAdd; 
	
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr;    
		n/=10;    
	}

	*(int*)content = schema->keySize; // Key Size of the attributes
	content = content + pageAdd;

	
	while(i < totalAttr){

		*(int*)content = (int) schema->typeLength[i]; // assign length of datatype of the attribute
		content = content + pageAdd;

		reslt=reslt+ptr; 

		*(int*)content = (int)schema->dataTypes[i]; // assign data type of attribute
		content = content + pageAdd;

		ptr = n+10% attributeLength;

		strncpy(content, schema->attrNames[i], attributeLength); // assign attribute name
		content = content + attributeLength;
		i++;
	}
	return RC_OK;
}

RC openSchemaTable(char *bufferHandle, Schema *schema) {
	table->totalRows= *(int*)bufferHandle; // get total number of rows from the page
	bufferHandle = bufferHandle + pageAdd;
	int n=attributeLength; 

	table->freePage= *(int*) bufferHandle; // get empty page from the page
    bufferHandle = bufferHandle + pageAdd;
	int reslt=0;
	int ptr;
	int i=0;
	int j=0;
	
	int totalAttribute = *(int*)bufferHandle; 	// retrieve the number of attributes from the page
	bufferHandle = bufferHandle + pageAdd;
 	
	while(n!=attributeLength)    
	{    
	 ptr=n%10;    
	 reslt=reslt*10+ptr;    
	 n/=10;    
	} 

	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *totalAttribute);
	schema->numAttr = totalAttribute;

	reslt=reslt+ptr; 
	ptr = n/attributeLength;

	schema->typeLength = (int*) malloc(pageAdd *totalAttribute);
	schema->attrNames = (char**) malloc(sizeof(char*) *totalAttribute);

	while(n!=attributeLength)    
	{           
		n/=10;    
	} 

	while(j<totalAttribute){
		schema->attrNames[j]= (char*) malloc(attributeLength); // Allocate memory space for storing attribute name
		j++;
	} 

	while(i<(schema->numAttr)){
		strncpy(schema->attrNames[i], bufferHandle, attributeLength); // assign attribute name
		bufferHandle = bufferHandle + attributeLength;

		while(n!=attributeLength)    
		{    
			ptr=n%10;    
			reslt=reslt*10+ptr; 
			ptr = n+10% attributeLength;
			ptr+=10; 
			n/=10;    
		}

		schema->typeLength[i]= *(int*)bufferHandle; // length of datatype is always eqaul to pagehandle ptr.	
		bufferHandle = bufferHandle + pageAdd;

		reslt=reslt+ptr; 
		ptr = n/attributeLength;

		schema->dataTypes[i] = *(int*) bufferHandle;
		bufferHandle = bufferHandle + pageAdd;
		i++;
	}

	return RC_OK;
}

// creates a table with name "name" and schema specified by "schema"
RC createTable(char *tableName, Schema *schema) {
	
	table = (Table*)malloc(sizeof(Table)); // Allocating memory space to the record manager
	int n=attributeLength; 


	initBufferPool(&table->bufferPool, tableName, 100, RS_LRU, NULL); // Initalizing the Buffer Pool call buffer manager
	int reslt=0;
	int ptr;

	createPageFile(tableName);
	
	SM_FileHandle fileHandle;
	openPageFile(tableName, &fileHandle);

	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;
		ptr+=10; 
		n/=10;    
	}

	char contents[PAGE_SIZE];
	createContent(contents, schema);
	reslt=reslt+ptr; 

	writeBlock(0, &fileHandle, contents);
	ptr = n/attributeLength;

	closePageFile(&fileHandle);

	return RC_OK;
}

// for deletes given name tables
RC deleteTable(char *tableName) {
	return destroyPageFile(tableName);
}

// opens the table of given table name "name"
RC openTable(RM_TableData *tData, char *tableName) {
	
	tData->mgmtData = table; // assign meta data of table to our record manager meta data
	tData->name = tableName;
	int n=attributeLength; 
	int reslt=0;
	int ptr; 
    
	pinPage(&table->bufferPool, &table->bufferHandle, 0); // call buffer manager to put page in buffer pool

	reslt=reslt+ptr; 
	ptr = n/attributeLength;

	SM_PageHandle bufferHandle = (char*) table->bufferHandle.data; // assign initial pointer as 0.
	Schema *schema = (Schema*) malloc(sizeof(Schema));

	openSchemaTable(bufferHandle, schema);
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;
		ptr+=10; 
		n/=10;    
	} 

	tData->schema = schema;	

	unpinPage(&table->bufferPool, &table->bufferHandle); 	// ptrove from Buffer Pool

	reslt=reslt+ptr; 
	forcePage(&table->bufferPool, &table->bufferHandle); 	// Write page to disk
	ptr = n/attributeLength;
	return RC_OK;
}   
  
// for closes the table using table metadata param
RC closeTable(RM_TableData *tData) {
	int n=attributeLength;
	Table *table = tData->mgmtData;	
	int reslt=0;
	tData->mgmtData = NULL;
	int ptr;
	reslt=reslt*10+ptr; 
	ptr = n+attributeLength;		
	shutdownBufferPool(&table->bufferPool);
	return RC_OK;
}


// for getting the number of records in the table
int getNumTuples(RM_TableData *tData) {
	Table *table = tData->mgmtData; // it's always equal to totalRows
	return table->totalRows;
}


/************************************************************
*             Record Handling Functions                              
************************************************************/

// returns a empty slot within a page
int findEmptySlot(char *data, int recordSize) {
	int i=0;
	while(i<(PAGE_SIZE / recordSize)){
		if (data[i * recordSize] != '+') 
			return i;
		i++;
	}
	return -1;
}

// for inserts a new record in the table
RC insertRecord(RM_TableData *tData, Record *record) {
	int n=attributeLength; 
	int reslt=0;
	Table *table = tData->mgmtData;	
	RID *ID = &record->id; 
	int ptr;
	
	
	ID->page = table->freePage;

	pinPage(&table->bufferPool, &table->bufferHandle, ID->page); //pin active page to bufferpool
	
	reslt=reslt*10+ptr; 
	ptr = n+10% attributeLength;
	int records = getRecordSize(tData->schema);
	
	char *data = table->bufferHandle.data;
	ptr+=10;
	ID->slot = findEmptySlot(data, records);
	
	bool isNotActive = ID->slot == -1;

	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;
		ptr+=10; 
		n/=10;    
	} 

	while (isNotActive) {
		unpinPage(&table->bufferPool, &table->bufferHandle);	
		ID->page++;

		if(n!=attributeLength){
			n=n%attributeLength;
		}
		pinPage(&table->bufferPool, &table->bufferHandle, ID->page); // take the new page into the BUffer Pool
		ptr = n+attributeLength;		

		data = table->bufferHandle.data;
		
		int slotCode = findEmptySlot(data, records);
		n=n*10+ptr;
		if (slotCode != -1) {
			isNotActive = false;
		}
		n=attributeLength;
		ID->slot = slotCode;
	}
	ptr=n-ptr;
	markDirty(&table->bufferPool, &table->bufferHandle); // Mark page was modified
	char *slotPos = data + (ID->slot * records); // slot starting position
	*slotPos = '+';

	ptr = n+attributeLength;
	memcpy(++slotPos, record->data + 1, records - 1);
	
	unpinPage(&table->bufferPool, &table->bufferHandle);
	reslt=reslt%10+ptr; 
	table->totalRows++;

	n = n%attributeLength;
	pinPage(&table->bufferPool, &table->bufferHandle, 0);

	return RC_OK;
}

// for deletes a record in the table
RC deleteRecord(RM_TableData *tData, RID ID) {
	Table *table = tData->mgmtData;
	int n=attributeLength; 
	int reslt=0;
	int ptr;
	
	pinPage(&table->bufferPool, &table->bufferHandle, ID.page);
	ptr=reslt%attributeLength;
	char *data  = table->bufferHandle.data + (ID.slot * (getRecordSize(tData->schema)));
	reslt*=attributeLength;
	*data = '-';

	table->freePage = ID.page;
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr;    
		n/=10;    
	}

	markDirty(&table->bufferPool, &table->bufferHandle);

	reslt=reslt%10+ptr; 
	ptr = n+attributeLength;
	unpinPage(&table->bufferPool, &table->bufferHandle);

	return RC_OK;
}

// for updates a record in the table
RC updateRecord(RM_TableData *tData, Record *record) {
	int n=attributeLength; 
	Table *table = tData->mgmtData;
	int reslt=0;
	int ptr;

	pinPage(&table->bufferPool, &table->bufferHandle, record->id.page);
	
	ptr = n+attributeLength;
	char *data = table->bufferHandle.data + (record->id.slot * getRecordSize(tData->schema));
	reslt=ptr*n;
	*data = '+';
	memcpy(++data, record->data + 1, getRecordSize(tData->schema) - 1 );
	
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr;    
		n/=10;    
	}    
	
	markDirty(&table->bufferPool, &table->bufferHandle);

	reslt=reslt%attributeLength+ptr; 
	unpinPage(&table->bufferPool, &table->bufferHandle);
	ptr = n+attributeLength;
	
	return RC_OK;	
}

// getting a record in the table 
RC getRecord(RM_TableData *tData, RID ID, Record *record) {
	int n=attributeLength; 
	Table *table = tData->mgmtData;
	int reslt=0;
	int ptr;
  	double arr[100];
	
	pinPage(&table->bufferPool, &table->bufferHandle, ID.page);

	ptr = n+attributeLength;
	char *dataPointer = table->bufferHandle.data + (ID.slot * getRecordSize(tData->schema));

	reslt=reslt*attributeLength+ptr; 
	ptr = n+attributeLength;

	if (*dataPointer != '+') {
		ptr+=10;
		return 305;
	} else {
		ptr+=20;
		record->id = ID;
		char *rData = record->data;

		memcpy(++rData, dataPointer + 1, getRecordSize(tData->schema) - 1);
	}

	for (int i = 0; i < n; i++) {
	    arr[i]= (i+2)%n;
	}
	unpinPage(&table->bufferPool, &table->bufferHandle);

	return RC_OK;
}



/************************************************************
*               Scan Functions                              *
************************************************************/


// for scaning all the records
RC startScan(RM_TableData *tData, RM_ScanHandle *sHandle, Expr *expr) {
	int reslt=0;
	if (expr == NULL) {
		return 304;
	}
	int n=attributeLength;
	int ptr;
	
	openTable(tData, "ScanTable");

	Table *r1 = (Table*) malloc(sizeof(Table));
	Table *r2 = tData->mgmtData;

	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;
		ptr+=10; 
		n/=10;    
	} 

	r2->totalRows = attributeLength;

	r1->totalReads = 0;
	r1->ID.slot = 0; 
	reslt=reslt*10+ptr; 

	r1->ID.page = 1;
	ptr = n+attributeLength;
	r1->expr = expr;

	sHandle->mgmtData = r1;
	sHandle->rel= tData;
	ptr = n%attributeLength;


	return RC_OK; 
}

// the next tuple that fulfills the scan expr.
RC next(RM_ScanHandle *sHandle, Record *record) {
	int n=attributeLength; 
	Table *scanM = sHandle->mgmtData;
	int reslt=0;
	int ptr;

	if (scanM->expr == NULL) {
		return 304;
	}

	Table *tableM = sHandle->rel->mgmtData;
	int totalRows = tableM->totalRows;

	ptr = n%attributeLength;
	if (totalRows == 0) return RC_RM_NO_MORE_TUPLES; 	// table has no more rows
	
	int totalReads = scanM->totalReads;
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;
		ptr+=10; 
		n/=10;    
	} 

	Schema *schema = sHandle->rel->schema;
	ptr = n/attributeLength;

	int records = getRecordSize(schema);

	while (totalReads <= totalRows) {  
		n=attributeLength;
		if (totalReads <= 0) {
			scanM->ID.slot = 0;
			scanM->ID.page = 1;
			ptr = n+10% attributeLength;
		} else {
			scanM->ID.slot++;
			n = n+10% attributeLength;
			if (scanM->ID.slot >= (PAGE_SIZE / records)) {
				scanM->ID.page++;
				ptr= n%attributeLength;
				scanM->ID.slot = 0;
			}
		}

		pinPage(&tableM->bufferPool, &scanM->bufferHandle, scanM->ID.page);	

		ptr = n% attributeLength;
		record->id.slot = scanM->ID.slot;

		n = n/ptr;
		record->id.page = scanM->ID.page;
		
		char *dataPointer = record->data;
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;

		char *data = scanM->bufferHandle.data + (scanM->ID.slot * records);
		n = ptr;

		*dataPointer = '-';
		ptr=ptr*attributeLength+ptr;
		memcpy(++dataPointer, data + 1, records - 1);

		scanM->totalReads++;
		
		Value *value = (Value *) malloc(sizeof(Value));
		reslt=reslt*10+ptr; 

		totalReads++;
		evalExpr(record, schema, scanM->expr, &value); 
		ptr = n+attributeLength;


		if(value->v.boolV == TRUE) {
			ptr=ptr*attributeLength+ptr;
			unpinPage(&tableM->bufferPool, &scanM->bufferHandle);		
			return RC_OK;
		}
	}
	
	
	scanM->ID.slot = 0;
	ptr = n+10% attributeLength;
	scanM->totalReads = 0;

	reslt=reslt+ptr; 
	scanM->ID.page = 1;

	n = n% attributeLength;
	unpinPage(&tableM->bufferPool, &scanM->bufferHandle);
	
	return RC_RM_NO_MORE_TUPLES;
}

// close scan operation.
RC closeScan(RM_ScanHandle *sHandle) {
	int n=attributeLength; 
	Table *tMgr = sHandle->mgmtData;
	int reslt=0;
	int ptr;

	if (tMgr->totalReads <= 0) {
		sHandle->mgmtData = NULL;
		while(n!=attributeLength)    
		{    
			ptr=n%10;    
			reslt=reslt*10+ptr; 
			ptr = n+10% attributeLength;
			ptr+=10; 
			n/=10;    
		} 
		free(sHandle->mgmtData); 
		n=attributeLength;
		return RC_OK;
		
	} else if (tMgr->totalReads > 0) {
		unpinPage(&table->bufferPool, &tMgr->bufferHandle);
		reslt=reslt*10+ptr; 
		reslt=1;
		tMgr->ID.slot = 0;
		
		ptr = n% attributeLength;
		ptr-=ptr;
		tMgr->totalReads = ptr;

		tMgr->ID.page = reslt;

		free(sHandle->mgmtData); 

		sHandle->mgmtData = NULL;
	}

	return RC_OK;
}

/************************************************************
*     				Dealing with schemas
************************************************************/
int getRecordSize(Schema *schema) {
	int count = 0;
	int i=0;

	while(i < schema->numAttr){
		int type = schema->dataTypes[i];

		switch(type){
			case DT_FLOAT: 	count = count + sizeof(float);
						break;
			case DT_BOOL:	count = count + sizeof(bool);
						break;
			case DT_STRING:	count = count + schema->typeLength[i];
						break;
			case DT_INT:	count = count + pageAdd;
						break;
		}
		i++;
	}

	return ++count;
}

// creates a new schema
Schema *createSchema(int totalAttr, char **attributeName, DataType *dt, int *dataTypeLength, int keyLength, int *keys) {
	int n=attributeLength; 
	Schema *schema = (Schema *) malloc(sizeof(Schema));
	int reslt=0;
	int ptr;

	Schema *nSchema = (Schema *) malloc(sizeof(Schema));
	if (keys == NULL) {
		nSchema->keyAttrs = NULL;
	}

	schema->keyAttrs = keys;
	if(n!=attributeLength){
		n+=attributeLength;
	}
	if (attributeName == NULL) {
		nSchema->attrNames = NULL;
	}

	schema->attrNames = attributeName;
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;
		ptr+=10; 
		n/=10;    
	}
	schema->numAttr = totalAttr;

	if (dt == NULL) {
		nSchema->dataTypes = NULL;
	}

	schema->typeLength = dataTypeLength;
		
	reslt=reslt+ptr; 
	schema->dataTypes = dt;

	if (keyLength) {
		nSchema->keySize = keyLength;
	}

	schema->keySize = keyLength;
	ptr = n% attributeLength;
	free(nSchema);

	return schema; 
}

// free schema memory
RC freeSchema(Schema *schema) {
	free(schema);
	return RC_OK;
}

/************************************************************
*     Dealing with records and attribute values
************************************************************/
RC createRecord(Record **record, Schema *schema) {
	int n=attributeLength; 
	
	*record = (Record*) malloc(sizeof(Record));
	int reslt=0;
	int ptr;

	(*record)->id.slot = -1;
	(*record)->id.page = -1;
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;
		ptr+=10; 
		n/=10;    
	} 
	
	(*record)->data= (char*) malloc(getRecordSize(schema));
	char *dataPointer = (*record)->data;
	reslt=reslt+ptr; 
	*dataPointer = '-'; // assign '-' for the empty record.
	*(++dataPointer) = '\0';

	return RC_OK;
}

// free record memory
RC freeRecord(Record *record) {

	if (record->data == NULL) {
		return RC_OK;
	}
	int n=attributeLength; 
	int reslt=0;
	int ptr;    
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr;    
		n/=10;    
	} 
	free(record->data);
	ptr=n%attributeLength;    
	record->data=NULL;
	n=attributeLength;
	return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int totalAttr, Value **value) {
	int n=attributeLength; 
	int i=0;
	int reslt=0;
	int count = 1;
	int ptr;
	bool res;
	float res1;
	int res2;
	
	while(i < totalAttr){
		int type = schema->dataTypes[i];

		switch(type){
			case DT_FLOAT: 	count = count + sizeof(float);
						break;
			case DT_BOOL:	count = count + sizeof(bool);
						break;
			case DT_STRING:	count = count + schema->typeLength[i];
						break;
			case DT_INT:	count = count + pageAdd;
						break;
		}
		i++;
	}

	if (totalAttr == 1) {
		schema->dataTypes[totalAttr] = 1;
	} 
	
	*value = (Value*) malloc(sizeof(Value));
	while(n!=attributeLength)    
	{    
		ptr=n%10;    
		reslt=reslt*10+ptr; 
		ptr = n+10% attributeLength;
		ptr+=10; 
		n/=10;    
	} 
	char *dataPointer = record->data + count;

	int dt = schema->dataTypes[totalAttr];

	switch(dt){
		case DT_BOOL:
						memcpy(&res, dataPointer, sizeof(bool));
						
						(*value)->dt = DT_BOOL;
						(*value)->v.boolV = res;
						break;
		case DT_FLOAT:
						memcpy(&res1, dataPointer, sizeof(float));
						
						(*value)->dt = DT_FLOAT;
						(*value)->v.floatV = res1;
						break;
		case DT_STRING:
						(*value)->v.stringV = (char *) malloc((schema->typeLength[totalAttr]) + 1);
						reslt=reslt*10+ptr; 
						strncpy((*value)->v.stringV, dataPointer, schema->typeLength[totalAttr]);
						ptr = n+10% attributeLength;
						(*value)->dt = DT_STRING;
						(*value)->v.stringV[schema->typeLength[totalAttr]] = '\0';
						break;
		case DT_INT:
						memcpy(&res2, dataPointer, pageAdd);
						
						(*value)->dt = DT_INT;
						(*value)->v.intV = res2;
						break;
	}

	return RC_OK;
}

/************************************************************
*     Dealing with records and attribute values
************************************************************/

RC setAttr(Record *record, Schema *schema, int totalAttr, Value *value) {

	int count = 1;
	int i=0;

	while(i < totalAttr){
		int type = schema->dataTypes[i];

		switch(type){
			case DT_FLOAT: 	count = count + sizeof(float);
						break;
			case DT_BOOL:	count = count + sizeof(bool);
						break;
			case DT_STRING:	count = count + schema->typeLength[i];
						break;
			case DT_INT:	count = count + pageAdd;
						break;
		}
		i++;
	}

	char *dataPointer = record->data + count;
	int dt = schema->dataTypes[totalAttr];
	switch(dt){
		case DT_BOOL:
						*(bool *) dataPointer = value->v.boolV;
						dataPointer = dataPointer + sizeof(bool);
						break;
		case DT_FLOAT:
						*(float *) dataPointer = value->v.floatV;
						dataPointer = dataPointer + sizeof(float);
						break;
		case DT_STRING:
						strncpy(dataPointer, value->v.stringV, schema->typeLength[totalAttr]);
						dataPointer = dataPointer + schema->typeLength[totalAttr];
						break;
		case DT_INT:
						*(int *) dataPointer = value->v.intV;	  
						dataPointer = dataPointer + pageAdd;
						break;
	}
		
	return RC_OK;
}