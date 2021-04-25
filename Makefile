precommit: test

SQLITE_TAGS="sqlite_icu sqlite_foreign_keys sqlite_json sqlite_fts5"

test:
	@ go test --tags $(SQLITE_TAGS) ./migrate_test.go

install: test
	@ go install --tags $(SQLITE_TAGS) ./cmd/...
