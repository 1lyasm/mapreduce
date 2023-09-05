all:
	./build.sh $(APP) $(FSANITIZE)
test:
	./test.sh
format:
	clang-format -i -style="{BasedOnStyle: google, IndentWidth: 4}" src/*/*.c src/*/*.h
clean:
	rm -rf mr-tmp bin perf*
