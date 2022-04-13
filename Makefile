assignment3: test_assign3_1.o dberror.o test_expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o expr.o
	gcc -w -o test_assign3_1 dberror.c record_mgr.c rm_serializer.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_assign3_1.c expr.c
	gcc -w -o test_expr dberror.c record_mgr.c rm_serializer.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_expr.c expr.c

%.o: %.c
	gcc -w -g -c $<

run:
	./test_assign3_1

run_expr:
	./test_expr

clean:
	rm -rf test_assign3_1 test_expr *.o