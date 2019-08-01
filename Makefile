POSTGREFLAGS=-lpq -I /Users/stephan/homebrew/Cellar/libpq/11.4/include/
EXPFLAGS=-L/Users/stephan/homebrew/opt/llvm/lib -lstdc++fs -lc++experimental
CC=g++-9
CFLAGS=-O3 -march=native -std=c++17 -Wall -Wextra -pedantic
LDFLAGS=-ldl -lpthread $(POSTGREFLAGS) $(EXPFLAGS)
SOURCES=$(wildcard *.cpp)
HEADERS=$(wildcard *.hpp) constants.h
OBJECTS=$(SOURCES:.cpp=.o)
NAME=gacspp

all: $(SOURCES) $(NAME)
    
$(NAME): $(OBJECTS) 
	$(CC) $(LDFLAGS) $(CFLAGS) sqlite3.o $(OBJECTS) -o $@

%.o: %.cpp $(HEADERS)
	$(CC) $(LDFLAGS) -c $(CFLAGS) $< -o $@

clean:
	rm -f $(OBJECTS)

fclean: clean
	rm -f $(NAME)

re: fclean all

sqlite3:
	gcc -O3 -march=native -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_RTREE=1 -c sqlite3.c

.PHONY: gacspp
