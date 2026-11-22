//go:build !nativecsv || !linux || !amd64 || !cgo

package main

func scanNativeEvents(string, window, *identities, catalog, func(inputEvent)) bool {
	return false
}
