The code execution for Record Manager starts with building the code with Make file which is responsible for creating the test_assign3_1 Binary file and linking test_assign3_1.c file with all corresponding *.h and *.c files. Make file inter-links each of the files in the directory with each other.

Run the following commands on a linux System. 

```    
    Step 1 : $ make
    Step 2 : $ make run
    Step 3 : $ make run_expr
```

Structure of Record Manager

buffer_mgr.h --> It has definition for various structures and functions to be used by buffer_mgr.c. 

buffer_mgr.c --> It is the main file which contains the Buffer manager function used to initialize buffer as per the number of PageFrames. It is used to copy file in pages from disk to PageFrame. It checks for fixedcount of Pages and Dirty bits to write the edited page back to disk. It contain FIFO and LRU agorithm to evict pages based on algorithm strategy if page frames are full.

buffer_mgr_stat.c --> This file contains the BufferPool statistic functions.

buffer_mgr_stat.h --> his file contains the BufferPool statistic functions definitions.

dberror.c --> It contains printerror and errormessage function. For the specific error it outputs the error code and specific error message

dberror.h --> It defines page_size constant as 4096 and definition for RC(return codes) for various File and Block functions in storage_mgr.
 
dt.h --> It contains constant variables for True and False. 
 
storage_mgr.h--> This is the file responsible for containing various file and block functions which are called from test_assign1_1.c It contains read and write function from file on disk to buffer and vise-versa. It contains creating, opening and closing file functions as well. It implements dberror.h and test_helpers.h file.
 
test_helper.h -> The file contains validation tasks for each function that are called within test_assign1. It is responsible for printing the RC code and RC message passed based on function's success or failure depnding on the values being passed to it.

test_expr.c -> This file tests the serialize and descerialize value of tuples, compares value and boolean opeartors as well as test complex expressions. 
1 extra test case is implemented in test_expr file.

tables.h -> This file defines the basic structure for schemas, tables, records, records ids and values. It contains functions to serialize data structures as string.

re_serializer.c -> This file conatins functions to serialize tuples of table for various table fucntions like scan, update, and delete. It seralizes schema as per different datatypes. It contains functions for seralizing records and attributes of table. Serializes data value as per different datatype.

record_mgr.c -> It is the main file that conatins the all the functions for record manager. We can create, update, delete and insert in tables. We can search records within a table using scan functions. Creating table schema as well as initiating and shutting down record manager.
