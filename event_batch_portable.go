//go:build !nativecsv || !linux || !amd64 || !cgo

package main

func scanNativeEvents(string, *timestampDecoder, window, *identities, catalog, func(inputEvent)) bool {
	return false
}
