all:	test_assign3
test_assign3: test_assign3_1.c 
	gcc dberror.c storage_mgr.c test_assign3_1.c -o test_assign3_1 buffer_mgr.c dt.h buffer_mgr_stat.c record_mgr.c tables.h expr.c expr.h test_helper.h rm_serializer.c -lm
run:
	./test_assign3_1

run_expr:
	./test_expr

clean:
	rm -rf test_assign3_1 test_expr *.o