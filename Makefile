precommit: test

SQLITE_TAGS="sqlite_icu sqlite_foreign_keys sqlite_json sqlite_fts5"

test:
	@ go vet --tags $(SQLITE_TAGS) ./...
	@ go run honnef.co/go/tools/cmd/staticcheck@latest ./...
	@ go test -race --tags $(SQLITE_TAGS) ./...

install: test
	@ go install --tags $(SQLITE_TAGS) ./cmd/...

release: VERSION := $(shell awk '/[0-9]+\.[0-9]+\.[0-9]+/ {print $$2; exit}' Changelog.md)
release: test install
	@ go mod tidy
	@ test -n "$(VERSION)" || (echo "Unable to read the version." && false)
	@ test -z "`git tag -l v$(VERSION)`" || (echo "Aborting because the v$(VERSION) tag already exists." && false)
	@ test -z "`git status --porcelain | grep -vE 'M (Changelog\.md)'`" || (echo "Aborting from uncommitted changes." && false)
	@ test -n "`git status --porcelain | grep -v 'M (Changelog\.md)'`" || (echo "Changelog.md must have changes" && false)
	@ git commit -am "Release v$(VERSION)"
	@ git tag "v$(VERSION)"
	@ git push origin main "v$(VERSION)"
	@ go run github.com/cli/cli/v2/cmd/gh@latest release create --generate-notes "v$(VERSION)"
