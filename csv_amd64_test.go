package main

import (
	"math/rand/v2"
	"slices"
	"testing"

	"golang.org/x/sys/cpu"
)

func TestAVX2CSVMasks(t *testing.T) {
	if !cpu.X86.HasAVX2 {
		t.Skip("AVX2 unavailable")
	}
	random := rand.New(rand.NewPCG(51, 18))
	text := make([]byte, 512*csvBlock)
	for i := range text {
		text[i] = byte(random.Uint32())
	}
	want, got := make([]csvMask, 512), make([]csvMask, 512)
	fillCSVMasksPortable(string(text), want)
	fillCSVMasksAVX2(string(text), got)
	if !slices.Equal(got, want) {
		t.Fatal("AVX2 and portable masks differ")
	}
}
