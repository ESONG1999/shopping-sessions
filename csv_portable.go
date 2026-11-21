//go:build !amd64

package main

func fillCSVMasks(text string, masks []csvMask) {
	fillCSVMasksPortable(text, masks)
}
