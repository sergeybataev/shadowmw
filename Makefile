.PHONY: build test race vet fuzz-smoke fmt lint-go

MODULES := . contrib/fx contrib/otel

build:
	@for mod in $(MODULES); do \
		(cd $$mod && go mod tidy && git diff --exit-code -- go.mod go.sum) || exit 1; \
	done
	gofmt -l .

fmt:
	gofmt -w .

test:
	@for mod in $(MODULES); do \
		(cd $$mod && go test -count=1 ./...) || exit 1; \
	done

race:
	@for mod in $(MODULES); do \
		(cd $$mod && go test -race -count=1 ./...) || exit 1; \
	done

vet:
	@for mod in $(MODULES); do \
		(cd $$mod && go vet ./...) || exit 1; \
	done

fuzz-smoke:
	go test -run='^$$' -fuzz='^FuzzOutcomeDifferenceRendering$$' -fuzztime=5s .
	go test -run='^$$' -fuzz='^FuzzRequestBodyParsing$$' -fuzztime=5s .
	go test -run='^$$' -fuzz='^FuzzResponseParsing$$' -fuzztime=5s .
	go test -run='^$$' -fuzz='^FuzzJSONPointerValidation$$' -fuzztime=5s .
	go test -run='^$$' -fuzz='^FuzzFormComparison$$' -fuzztime=5s .
	go test -run='^$$' -fuzz='^FuzzContentDecoding$$' -fuzztime=5s .
	go test -run='^$$' -fuzz='^FuzzInvalidComparisonInput$$' -fuzztime=5s .
	go test -run='^$$' -fuzz='^FuzzMiddlewareMalformedAcceptQualityDoesNotBypass$$' -fuzztime=5s .

lint-go:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		for mod in $(MODULES); do \
			(cd $$mod && golangci-lint run --config $(CURDIR)/.golangci.yml ./...) || exit 1; \
		done; \
	else \
		$(MAKE) vet; \
	fi
