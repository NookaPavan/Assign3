
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

typedef struct TableMgr {
	int rawCount; // total number of rows in the table
	int emptyPage;    // first free page
	int scanRecords;   // number of records scanned

	BM_PageHandle pHandle;	// Buffer Manager PageHandle 	
	BM_BufferPool bPool;   // bufferpool	

	RID rID;
	Expr *expr;
} TableMgr;

TableMgr *tableManager;

/************************************************************
*         Table and Record Manager Functions.               *
************************************************************/

// Initiliazing Storage Manager
RC initRecordManager(void *mgmtData) {
	printf("Initializing Record Manager \n");
	
	initStorageManager();
	
	return RC_OK;
}

// shuts down the Record Manager
RC shutdownRecordManager() {
	free(tableManager);
	return RC_OK;
}

int attrLength = 20;
long pageAdd = sizeof(int);

RC createContent(char *data, Schema *schema) {
	char *content = data;
	int totalAttr = schema->numAttr;
	content = content + pageAdd;

	*(int*)content = 1; 
	content = content + pageAdd; 

	*(int*)content = schema->numAttr; // Setting the number of attributes
	content = content + pageAdd; 

	*(int*)content = schema->keySize; // Key Size of the attributes
	content = content + pageAdd;
	
	for (int i = 0; i < totalAttr; i++) {

		*(int*)content = (int) schema->typeLength[i]; // assign length of datatype of the attribute
		content = content + pageAdd;

		*(int*)content = (int)schema->dataTypes[i]; // assign data type of attribute
		content = content + pageAdd;

		strncpy(content, schema->attrNames[i], attrLength); // assign attribute name
		content = content + attrLength;
	}
	return RC_OK;
}

RC openSchemaTable(char *pHandle, Schema *schema) {
	tableManager->rawCount= *(int*)pHandle; // get total number of rows from the page
	pHandle = pHandle + pageAdd;

	tableManager->emptyPage= *(int*) pHandle; // get empty page from the page
    pHandle = pHandle + pageAdd;
	
	int totalAttribute = *(int*)pHandle; 	// retrieve the number of attributes from the page
	pHandle = pHandle + pageAdd;
 	
	
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *totalAttribute);
	schema->numAttr = totalAttribute;
	schema->typeLength = (int*) malloc(pageAdd *totalAttribute);
	schema->attrNames = (char**) malloc(sizeof(char*) *totalAttribute);

	for (int i = 0; i < totalAttribute; i++) {
		schema->attrNames[i]= (char*) malloc(attrLength); // Allocate memory space for storing attribute name
	}
      
	for (int i = 0; i < schema->numAttr; i++) {
		strncpy(schema->attrNames[i], pHandle, attrLength); // assign attribute name
		pHandle = pHandle + attrLength;

		schema->typeLength[i]= *(int*)pHandle; // length of datatype is always eqaul to pagehandle ptr.	
		pHandle = pHandle + pageAdd;

		schema->dataTypes[i] = *(int*) pHandle;
		pHandle = pHandle + pageAdd;

	}

	return RC_OK;
}

// creates a table with name "name" and schema specified by "schema"
RC createTable(char *tableName, Schema *schema) {
	
	tableManager = (TableMgr*)malloc(sizeof(TableMgr)); // Allocating memory space to the record manager

	initBufferPool(&tableManager->bPool, tableName, 100, RS_LRU, NULL); // Initalizing the Buffer Pool call buffer manager

	createPageFile(tableName);
	
	SM_FileHandle fileHandle;
	openPageFile(tableName, &fileHandle);

	char contents[PAGE_SIZE];
	createContent(contents, schema);

	writeBlock(0, &fileHandle, contents);
	closePageFile(&fileHandle);

	return RC_OK;
}

// opens the table of given table name "name"
RC openTable(RM_TableData *tData, char *tableName) {
	
	tData->mgmtData = tableManager; // assign meta data of table to our record manager meta data
	tData->name = tableName;
    
	pinPage(&tableManager->bPool, &tableManager->pHandle, 0); // call buffer manager to put page in buffer pool

	SM_PageHandle pHandle = (char*) tableManager->pHandle.data; // assign initial pointer as 0.
	Schema *schema = (Schema*) malloc(sizeof(Schema));

	openSchemaTable(pHandle, schema);
	
	tData->schema = schema;	

	unpinPage(&tableManager->bPool, &tableManager->pHandle); 	// remove from Buffer Pool

	forcePage(&tableManager->bPool, &tableManager->pHandle); 	// Write page to disk

	return RC_OK;
}   
  
// for closes the table using table metadata param
RC closeTable(RM_TableData *tData) {
	TableMgr *tableManager = tData->mgmtData;	
	tData->mgmtData = NULL;
	shutdownBufferPool(&tableManager->bPool);
	return RC_OK;
}

// for deletes given name tables
RC deleteTable(char *tableName) {
	return destroyPageFile(tableName);
}

// for getting the number of records in the table
int getNumTuples(RM_TableData *tData) {
	TableMgr *tableManager = tData->mgmtData; // it's always equal to rawCount
	return tableManager->rawCount;
}


/************************************************************
*             Record Functions                              *
************************************************************/

// returns a empty slot within a page
int findEmptySlot(char *data, int recordSize) {
	for (int i = 0; i < (PAGE_SIZE / recordSize); i++) {
		if (data[i * recordSize] != '+') return i;
	}
	return -1;
}

// for inserts a new record in the table
RC insertRecord(RM_TableData *tData, Record *record) {
	TableMgr *tableManager = tData->mgmtData;	
	RID *rID = &record->id; 
	
	
	rID->page = tableManager->emptyPage;

	pinPage(&tableManager->bPool, &tableManager->pHandle, rID->page); //pin active page to bufferpool
	
	int records = getRecordSize(tData->schema);
	
	char *data = tableManager->pHandle.data;
	rID->slot = findEmptySlot(data, records);
	
	bool isNotActive = rID->slot == -1;

	while (isNotActive) {
		unpinPage(&tableManager->bPool, &tableManager->pHandle);	
		rID->page++;

		pinPage(&tableManager->bPool, &tableManager->pHandle, rID->page); // take the new page into the BUffer Pool		

		data = tableManager->pHandle.data;
		
		int slotCode = findEmptySlot(data, records);
		if (slotCode != -1) {
			isNotActive = false;
		}
		
		rID->slot = slotCode;
	}
	
	
	markDirty(&tableManager->bPool, &tableManager->pHandle); // Mark page was modified
	char *slotPos = data + (rID->slot * records); // slot starting position
	*slotPos = '+';

	memcpy(++slotPos, record->data + 1, records - 1);
	
	unpinPage(&tableManager->bPool, &tableManager->pHandle);
	
	tableManager->rawCount++;
	pinPage(&tableManager->bPool, &tableManager->pHandle, 0);

	return RC_OK;
}

// for deletes a record in the table
RC deleteRecord(RM_TableData *tData, RID rID) {
	TableMgr *tableManager = tData->mgmtData;
	
	pinPage(&tableManager->bPool, &tableManager->pHandle, rID.page);

	char *data  = tableManager->pHandle.data + (rID.slot * (getRecordSize(tData->schema)));
	*data = '-';

	tableManager->emptyPage = rID.page;

	markDirty(&tableManager->bPool, &tableManager->pHandle);

	unpinPage(&tableManager->bPool, &tableManager->pHandle);

	return RC_OK;
}

// for updates a record in the table
RC updateRecord(RM_TableData *tData, Record *record) {	
	TableMgr *tableManager = tData->mgmtData;

	pinPage(&tableManager->bPool, &tableManager->pHandle, record->id.page);
	char *data = tableManager->pHandle.data + (record->id.slot * getRecordSize(tData->schema));
	*data = '+';
	
	memcpy(++data, record->data + 1, getRecordSize(tData->schema) - 1 );
	
	markDirty(&tableManager->bPool, &tableManager->pHandle);

	unpinPage(&tableManager->bPool, &tableManager->pHandle);
	
	return RC_OK;	
}

// getting a record in the table 
RC getRecord(RM_TableData *tData, RID rID, Record *record) {
	TableMgr *tableManager = tData->mgmtData;
	
	pinPage(&tableManager->bPool, &tableManager->pHandle, rID.page);

	char *dataPointer = tableManager->pHandle.data + (rID.slot * getRecordSize(tData->schema));

	if (*dataPointer != '+') {
		return 305; // no matching record for 'id' in the table
	} else {
		record->id = rID;
		char *rData = record->data;

		memcpy(++rData, dataPointer + 1, getRecordSize(tData->schema) - 1);
	}

	unpinPage(&tableManager->bPool, &tableManager->pHandle);

	return RC_OK;
}



/************************************************************
*               Scan Functions                              *
************************************************************/


// for scaning all the records
RC startScan(RM_TableData *tData, RM_ScanHandle *sHandle, Expr *expr) {
	if (expr == NULL) {
		return 304;
	}
	
	openTable(tData, "ScanTable");

	TableMgr *r1 = (TableMgr*) malloc(sizeof(TableMgr));
	TableMgr *r2 = tData->mgmtData;

	r2->rawCount = attrLength;

	r1->scanRecords = 0;
	r1->rID.slot = 0; 

	r1->rID.page = 1;

	r1->expr = expr;

	sHandle->mgmtData = r1;
	sHandle->rel= tData;

	return RC_OK; 
}

// the next tuple that fulfills the scan expr.
RC next(RM_ScanHandle *sHandle, Record *record) {
	TableMgr *scanM = sHandle->mgmtData;

	if (scanM->expr == NULL) {
		return 304;
	}

	TableMgr *tableM = sHandle->rel->mgmtData;
	int rawCount = tableM->rawCount;

	if (rawCount == 0) return RC_RM_NO_MORE_TUPLES; 	// table has no more rows
	
	int scanRecords = scanM->scanRecords;

	Schema *schema = sHandle->rel->schema;
	int records = getRecordSize(schema);

	while (scanRecords <= rawCount) {  

		if (scanRecords <= 0) {
			scanM->rID.slot = 0;
			scanM->rID.page = 1;
		} else {
			scanM->rID.slot++;
			if (scanM->rID.slot >= (PAGE_SIZE / records)) {
				scanM->rID.page++;
				scanM->rID.slot = 0;
			}
		}

		pinPage(&tableM->bPool, &scanM->pHandle, scanM->rID.page);	

		
		record->id.slot = scanM->rID.slot;
		record->id.page = scanM->rID.page;
		
		char *dataPointer = record->data;
		char *data = scanM->pHandle.data + (scanM->rID.slot * records);
		*dataPointer = '-';
		memcpy(++dataPointer, data + 1, records - 1);

		scanM->scanRecords++;
		scanRecords++;

		Value *value = (Value *) malloc(sizeof(Value));
		evalExpr(record, schema, scanM->expr, &value); 

		if(value->v.boolV == TRUE) {
			unpinPage(&tableM->bPool, &scanM->pHandle);		
			return RC_OK;
		}
	}
	
	
	scanM->rID.slot = 0;
	scanM->scanRecords = 0;
	scanM->rID.page = 1;

	unpinPage(&tableM->bPool, &scanM->pHandle);
	
	return RC_RM_NO_MORE_TUPLES;
}

// close scan operation.
RC closeScan(RM_ScanHandle *sHandle) {
	TableMgr *tMgr = sHandle->mgmtData;

	if (tMgr->scanRecords <= 0) {
		sHandle->mgmtData = NULL;
		free(sHandle->mgmtData); 

		return RC_OK;
		
	} else if (tMgr->scanRecords > 0) {
		unpinPage(&tableManager->bPool, &tMgr->pHandle);

		tMgr->rID.slot = 0;
		tMgr->scanRecords = 0;
		tMgr->rID.page = 1;
		
		free(sHandle->mgmtData); 

		sHandle->mgmtData = NULL;
	}

	return RC_OK;
}


/************************************************************
*                 Schema Functions                         *
************************************************************/

// get the record size of the given schema 
int getRecordSize(Schema *schema) {
	int count = 0;
	for (int i = 0; i < schema->numAttr; i++) {
		int type = schema->dataTypes[i];
		if (type == DT_FLOAT) {
			count = count + sizeof(float);
		} else if (type == DT_BOOL) {
			count = count + sizeof(bool);
		} else if (type == DT_STRING) {
			count = count + schema->typeLength[i];
		} else if (type == DT_INT) {
			count = count + pageAdd;
		}
	}
	return ++count;
}

// creates a new schema
Schema *createSchema(int totalAttr, char **attributeName, DataType *dt, int *dataTypeLength, int keyLength, int *keys) {
	Schema *schema = (Schema *) malloc(sizeof(Schema));
	Schema *nSchema = (Schema *) malloc(sizeof(Schema));

	if (keys == NULL) {
		nSchema->keyAttrs = NULL;
	}

	schema->keyAttrs = keys;

	if (attributeName == NULL) {
		nSchema->attrNames = NULL;
	}

	schema->attrNames = attributeName;
	schema->numAttr = totalAttr;

	if (dt == NULL) {
		nSchema->dataTypes = NULL;
	}

	schema->typeLength = dataTypeLength;
	schema->dataTypes = dt;

	if (keyLength != NULL) {
		nSchema->keySize = keyLength;
	}

	schema->keySize = keyLength;
	
	free(nSchema);

	return schema; 
}

// free schema memory
RC freeSchema(Schema *schema) {
	if (schema == NULL) {
		return RC_OK;
	}
	
	free(schema);
	return RC_OK;
}


/************************************************************
*    dealing with records and attribute values              *
************************************************************/


// for creating new record
RC createRecord(Record **record, Schema *schema) {
	*record = (Record*) malloc(sizeof(Record));

	(*record)->id.slot = -1;
	(*record)->id.page = -1;
	(*record)->data= (char*) malloc(getRecordSize(schema));

	char *dataPointer = (*record)->data;
	*dataPointer = '-'; // assign '-' for the empty record.
	*(++dataPointer) = '\0';

	return RC_OK;
}

// free record memory
RC freeRecord(Record *record) {

	if (record->data == NULL) {
		return RC_OK;
	}

	free(record->data);
	record->data=NULL;

	return RC_OK;
}

// get an attribute from the given record in the specified schema
RC getAttr(Record *record, Schema *schema, int totalAttr, Value **value) {
	int count = 1;
	for (int i = 0; i < totalAttr; i++) {
		int type = schema->dataTypes[i];
		if (type == DT_BOOL) {
			count = count + sizeof(bool);
		} else if (type == DT_FLOAT) {
			count = count + sizeof(float);
		} else if (type == DT_STRING) {
			count = count + schema->typeLength[i];
		} else if (type == DT_INT) {
			count = count + pageAdd;
		}
	}

	if (totalAttr == 1) {
		schema->dataTypes[totalAttr] = 1;
	} 
	
	*value = (Value*) malloc(sizeof(Value));
	char *dataPointer = record->data + count;
	int dt = schema->dataTypes[totalAttr];

	if (dt == DT_BOOL) {

		bool res;
		memcpy(&res, dataPointer, sizeof(bool));

		(*value)->dt = DT_BOOL;
		(*value)->v.boolV = res;

	} else if (dt == DT_FLOAT) {

		float res;
		memcpy(&res, dataPointer, sizeof(float));

		(*value)->dt = DT_FLOAT;
		(*value)->v.floatV = res;

	} else if (dt == DT_STRING) {

		(*value)->v.stringV = (char *) malloc((schema->typeLength[totalAttr]) + 1);
		strncpy((*value)->v.stringV, dataPointer, schema->typeLength[totalAttr]);

		(*value)->dt = DT_STRING;
		(*value)->v.stringV[schema->typeLength[totalAttr]] = '\0';

	} else if (dt == DT_INT) {

		int res;
		memcpy(&res, dataPointer, pageAdd);

		(*value)->dt = DT_INT;
		(*value)->v.intV = res;
	}

	return RC_OK;
}

// sets the attribute value in the record
RC setAttr(Record *record, Schema *schema, int totalAttr, Value *value) {

	int count = 1;
	for (int i = 0; i < totalAttr; i++) {
		int type = schema->dataTypes[i];
		if (type == DT_BOOL) {
			count = count + sizeof(bool);
		} else if (type == DT_FLOAT) {
			count = count + sizeof(float);
		} else if (type == DT_STRING) {
			count = count + schema->typeLength[i];
		} else if (type == DT_INT) {
			count = count + pageAdd;
		}
	}

	char *dataPointer = record->data + count;
	int dt = schema->dataTypes[totalAttr];
	if (dt == DT_BOOL) {
		*(bool *) dataPointer = value->v.boolV;
		dataPointer = dataPointer + sizeof(bool);
	} else if (dt == DT_FLOAT) {
		*(float *) dataPointer = value->v.floatV;
		dataPointer = dataPointer + sizeof(float);
	} else if (dt == DT_STRING) {
		strncpy(dataPointer, value->v.stringV, schema->typeLength[totalAttr]);
		dataPointer = dataPointer + schema->typeLength[totalAttr];
	} else if (dt == DT_INT) {
		*(int *) dataPointer = value->v.intV;	  
		dataPointer = dataPointer + pageAdd;
	}
		
	return RC_OK;
}