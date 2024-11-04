.PHONY: build
build:
	codon build --release -o kura src/kura/main.py

.PHONY: run
run:
	codon run src/kura/main.py

.PHONY: lint/fix
lint/fix:
	rye lint --fix
	rye fmt

.PHONY: lint
lint:
	rye lint
	rye fmt --check
	rye run mypy --explicit-package-bases src tests

.PHONY: test
test:
	PYTHONPATH=. rye test
