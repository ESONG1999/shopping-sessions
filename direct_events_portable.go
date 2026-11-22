//go:build !nativecsv || !linux || !amd64 || !cgo

package main

type directEventLane struct{}

func allocateDirectLanes(mappedChunks, int) []directEventLane { return nil }
func (*directEventLane) scan(string, window, *identities, catalog, *eventLane, int64) bool {
	return false
}
func (*directEventLane) publish(*eventInput, int) {}
