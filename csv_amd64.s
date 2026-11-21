#include "textflag.h"

// One mask pair per 32 bytes. The caller supplies at least one complete block.
TEXT ·fillCSVMasksAVX2(SB), NOSPLIT, $0-40
	MOVQ text_base+0(FP), SI
	MOVQ masks_base+16(FP), DI
	MOVQ masks_len+24(FP), CX
	MOVL $44, AX
	MOVD AX, X1
	VPBROADCASTB X1, Y1
	MOVL $10, AX
	MOVD AX, X2
	VPBROADCASTB X2, Y2
	MOVL $34, AX
	MOVD AX, X3
	VPBROADCASTB X3, Y3

next:
	VMOVDQU (SI), Y0
	VPCMPEQB Y1, Y0, Y4
	VPCMPEQB Y2, Y0, Y5
	VPOR Y4, Y5, Y4
	VPCMPEQB Y3, Y0, Y5
	VPMOVMSKB Y4, AX
	VPMOVMSKB Y5, DX
	MOVL AX, (DI)
	MOVL DX, 4(DI)
	ADDQ $32, SI
	ADDQ $8, DI
	DECQ CX
	JNZ next
	VZEROUPPER
	RET
