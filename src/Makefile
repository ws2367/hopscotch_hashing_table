## DO NOT MODIFY THE CONTENTS OF THIS FILE

X10C=${X10_HOME}/bin/x10c++

##debugging
##FLAGS=-VERBOSE_CHECKS=TRUE -O -noassert -cxx-prearg -O2
##production
FLAGS=-NO_CHECKS -O -noassert -cxx-prearg -O2

TARBALL=${USER}_project2.tar.gz

SRCS=functests.x10 perftests.x10
TURNIN=$(SRCS) CHashMap.x10

EXES=$(SRCS:.x10=)

all: $(EXES)

.SUFFIXES:
.SUFFIXES: .x10

.x10:
	$(X10C) $(FLAGS) -o $@ $@.x10

turnin: $(TARBALL)

$(TARBALL): $(TURNIN)
	tar cvfz $(TARBALL) $(TURNIN)

clean:
	rm -f $(EXES) *.h *.cc $(TARBALL)
