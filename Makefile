
BINARIES:=$(subst /cmd/,/bin/,$(wildcard ./cmd/*))
GOCMD:=go
VERSION:=$(shell git describe --always)
PACKAGES:=$(shell go list ./... | grep -v /vendor/)
GO15VENDOREXPERIMENT=1

.PHONY: all test test-integration clean rpm $(BINARIES)

all: $(BINARIES)

test:
	$(GOCMD) test -v $(PACKAGES)

# use the command line flag -mysql to set the correct dial command
test-integration:
	$(GOCMD) test -tags=integration $(PACKAGES)

clean:
	rm -rf ./bin

./bin:
	mkdir -p ./bin

# the rpm target requires `fpm` to be installed
rpm: ./bin/bendo ./bin/bclient ./bin/butil
	fpm -t rpm -s dir \
		--name bendo \
		--version $(VERSION) \
		--vendor ndlib \
		--maintainer DLT \
		--description "Tape manager daemon" \
		--rpm-user app \
		--rpm-group app \
		bin/bendo=/opt/bendo/bin/bendo \
		bin/bclient=/opt/bendo/bin/bclient \
		bin/butil=/opt/bendo/bin/butil


# go will track changes in dependencies, so the makefile does not need to do
# that. That means we always compile everything here.
# Need to include initial "./" in path so go knows it is a relative package path.
$(BINARIES): ./bin/%: ./cmd/% | ./bin
	$(GOCMD) build -ldflags "-X github.com/ndlib/bendo/server.Version=$(VERSION)" \
		-o ./$@ ./$<
