POSTGREFLAGS=-lpq -I /Users/stephan/homebrew/Cellar/libpq/11.4/include/
EXPFLAGS=-L/Users/stephan/homebrew/opt/llvm/lib -lc++experimental
CC=g++-9
CFLAGS=-O3 -march=native -std=c++17 -Wall -Wextra -pedantic
LDFLAGS=-ldl -lpthread $(POSTGREFLAGS) $(EXPFLAGS)
SOURCES=main_droptables.cpp
HEADERS=
OBJECTS=$(SOURCES:.cpp=.o)
NAME=droptables

all: $(SOURCES) $(NAME)
    
$(NAME): $(OBJECTS) 
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

%.o: %.cpp $(HEADERS)
	$(CC) $(LDFLAGS) -c $(CFLAGS) $< -o $@

clean:
	rm -f $(OBJECTS)

fclean: clean
	rm -f $(NAME)

re: fclean all
