package retriever

import "net/url"

func graphDirectoryName(name string) string {
	escaped := url.PathEscape(name)
	if escaped == "" {
		return DefaultGraphName
	}

	return escaped
}
