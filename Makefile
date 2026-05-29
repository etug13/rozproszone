CC = mpicc
CFLAGS = -Wall -O2 -pthread
OBJ = main.o util.o queue.o
BIN = busy

all: $(BIN)

$(BIN): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ)

%.o: %.c common.h
	$(CC) $(CFLAGS) -c $<

run: $(BIN)
	mpirun -np 5 ./$(BIN)

clean:
	rm -f $(OBJ) $(BIN)
