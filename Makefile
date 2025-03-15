.PHONY: test bench clean

all: test

test:
	go test -v

bench:
	go test -bench=. -benchmem

example:
	cd example && go run main.go

clean:
	rm -f *.o *.so *.dylib *.dll *.test