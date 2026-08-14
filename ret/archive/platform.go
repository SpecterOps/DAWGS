package archive

import "fmt"

func requirePlatformSupport(platform string) error {
	switch platform {
	case "linux", "darwin":
		return nil
	default:
		return fmt.Errorf(
			"archive publication is unsupported on platform %q; supported platforms are linux and darwin",
			platform,
		)
	}
}
