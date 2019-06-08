precommit: test

test:
	@ rm tmp.db
	@ go test ./migrate_test.go

install: test
	@ go install ./cmd/...