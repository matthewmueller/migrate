precommit: test

SQLITE_TAGS="sqlite_icu sqlite_foreign_keys sqlite_json sqlite_fts5"

test:
	@ go vet --tags $(SQLITE_TAGS) ./...
	@ go run honnef.co/go/tools/cmd/staticcheck@latest ./...
	@ go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...
	@ go test -race --tags $(SQLITE_TAGS) ./...

install: test
	@ go install --tags $(SQLITE_TAGS) ./cmd/...

release: VERSION := $(shell awk '/[0-9]+\.[0-9]+\.[0-9]+/ {print $$2; exit}' Changelog.md)
release: VERSION_FILE := version.go
release: CHANGELOG_FILE := Changelog.md
release: test
	@ go mod tidy
	@ test -n "$(VERSION)" || (echo "Unable to read the version." && false)
	@ test -z "`git tag -l v$(VERSION)`" || (echo "Aborting because the v$(VERSION) tag already exists." && false)
	@ test -z "`git status --porcelain | grep -vE '(A|M|[\?]{2}) ($(CHANGELOG_FILE)|$(VERSION_FILE))'`" || (echo "Aborting from uncommitted changes." && false)
	@ test -n "`git status --porcelain | grep -v 'M ($(CHANGELOG_FILE))'`" || (echo "Changelog.md must have changes" && false)
	@ go run github.com/x-motemen/gobump/cmd/gobump@latest set $(VERSION) -w $(dir $(VERSION_FILE))
	@ test -n "`git status --porcelain | grep -v 'M ($(VERSION_FILE))'`" || (echo "$(VERSION_FILE) must have changes" && false)
	@ git add -A && git commit -m "Release v$(VERSION)"
	@ test -z "`git status --porcelain`" || (echo "Aborting from uncommitted changes." && false)
	@ git tag "v$(VERSION)"
	@ git push origin main "v$(VERSION)"
	@ go run github.com/cli/cli/v2/cmd/gh@latest release create --generate-notes "v$(VERSION)"
	@ go install --tags $(SQLITE_TAGS) ./cmd/...