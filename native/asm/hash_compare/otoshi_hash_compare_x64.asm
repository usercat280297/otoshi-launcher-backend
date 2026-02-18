; otoshi_hash_compare_x64.asm
; x64 routine for constant-time byte compare.
; Calling convention (Windows x64):
;   RCX = left pointer
;   RDX = right pointer
;   R8  = length
; Returns:
;   EAX = 1 if equal, 0 otherwise

PUBLIC otoshi_consttime_eq_x64

.code
otoshi_consttime_eq_x64 PROC
    xor     r9d, r9d            ; diff accumulator
    test    rcx, rcx
    jz      not_equal
    test    rdx, rdx
    jz      not_equal
    test    r8, r8
    jz      equal

compare_loop:
    mov     al, BYTE PTR [rcx]
    xor     al, BYTE PTR [rdx]
    or      r9b, al
    inc     rcx
    inc     rdx
    dec     r8
    jnz     compare_loop

    test    r9b, r9b
    jz      equal

not_equal:
    xor     eax, eax
    ret

equal:
    mov     eax, 1
    ret
otoshi_consttime_eq_x64 ENDP
END
