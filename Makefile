precommit: test

test:
	@ rm tmp.db
	@ go test ./migrate_test.go
	@ rm tmp.db

install: test
	@ go install ./cmd/...