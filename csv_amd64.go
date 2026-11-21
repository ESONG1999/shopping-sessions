package main

import "golang.org/x/sys/cpu"

func fillCSVMasks(text string, masks []csvMask) {
	if cpu.X86.HasAVX2 {
		fillCSVMasksAVX2(text, masks)
		return
	}
	fillCSVMasksPortable(text, masks)
}

//go:noescape
func fillCSVMasksAVX2(text string, masks []csvMask)
