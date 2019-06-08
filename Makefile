precommit: test

test:
	@ go test ./migrate_test.go

install: test
	@ go install ./cmd/...