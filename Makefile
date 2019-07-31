POSTGREFLAGS=-lpq -I ~/homebrew/Cellar/libpq/11.4/include/

gacspp:
	g++ -O3 -march=native -std=c++17 -Wall -Wextra -pedantic $(wildcard *.cpp) sqlite3.o -o gacspp.out -ldl -lpthread -lstdc++fs $(POSTGREFLAGS)

sqlite3:
	gcc -O3 -march=native -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_RTREE=1 -c sqlite3.c
.PHONY: gacspp
