ERLC ?= erlc
SRC  := $(wildcard src/*.erl)
BEAM := $(patsubst src/%.erl,ebin/%.beam,$(SRC))

all: $(BEAM)

ebin/%.beam: src/%.erl | ebin
	$(ERLC) -Wall -o ebin $<

ebin:
	mkdir -p ebin

clean:
	rm -rf ebin

.PHONY: all clean
