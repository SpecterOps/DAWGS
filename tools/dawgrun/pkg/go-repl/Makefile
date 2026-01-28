all: ./examples/basic_repl

#./examples/basic_repl: ./*.go ./examples/basic_repl.go
	#cd ./examples; \
	#go build basic_repl.go

./examples/%: ./examples/%.go ./*.go 
	cd ./examples; \
	go build $(notdir $<)

test: ./examples/basic_repl
	./examples/basic_repl

test-shell_wrapper: ./examples/shell_wrapper
	./examples/shell_wrapper
