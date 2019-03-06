package dedent

import (
	"regexp"
	"strings"
)

var whitespaceOnly = regexp.MustCompile("(?m)^[ \t]+$")
var leadingWhitespace = regexp.MustCompile("(?m)(^[ \t]*)(?:[^ \t\n])")

// String dedent
func String(text string) string {
	var margin string
	text = whitespaceOnly.ReplaceAllString(text, "")
	indents := leadingWhitespace.FindAllStringSubmatch(text, -1)
	for i, indent := range indents {
		if i == 0 {
			margin = indent[1]
		} else if strings.HasPrefix(indent[1], margin) {
			continue
		} else if strings.HasPrefix(margin, indent[1]) {
			margin = indent[1]
		} else {
			margin = ""
			break
		}
	}
	if margin != "" {
		text = regexp.MustCompile("(?m)^"+margin).ReplaceAllString(text, "")
	}
	return text
}
