/* Generic linker script for stateless PIC modules (all architectures) */
/* Code runs from flash (XIP on RP) or embedded blob (aarch64). */
/* State is passed via pointer from kernel — no RAM section needed. */
/* Mirrors clustor/modules/module.ld — keep these in sync.            */
ENTRY(module_init)

MEMORY
{
    /* Per-module size cap. Not a real flash budget on any target —
     * bcm2712 mmaps modules from RAM, rp2040/rp2350 XIP-image hosts
     * multi-MB flash. The cap exists purely as a "modules should be
     * small" guideline.
     */
    FLASH (rx)  : ORIGIN = 0x00000000, LENGTH = 512K
}

SECTIONS
{
    .text : {
        _code_start = .;
        KEEP(*(.text.module_state_size*))
        KEEP(*(.text.module_init*))
        KEEP(*(.text.module_new*))
        KEEP(*(.text.module_step*))
        KEEP(*(.text.module_channel_hints*))
        KEEP(*(.text.module_in_place_safe*))
        KEEP(*(.text.module_mailbox_safe*))
        KEEP(*(.text.module_deferred_ready*))
        KEEP(*(.text.module_drain*))
        KEEP(*(.text.module_arena_size*))
        KEEP(*(.text.module_provider_dispatch*))
        KEEP(*(.text.module_provides_contract*))
        KEEP(*(.text.module_flash_store_dispatch*))
        KEEP(*(.text.module_isr_init*))
        KEEP(*(.text.module_isr_entry*))
        KEEP(*(.text.backing_provider_dispatch*))
        *(.text*)
        . = ALIGN(8);
    } > FLASH

    .rodata : {
        *(.rodata*)
        /* LLVM-emitted match-dispatch tables land in .data.rel.ro;
         * funnel them here so the loader relocates them as rodata. */
        *(.data.rel.ro*)
        . = ALIGN(8);
        _code_end = .;
    } > FLASH

    .data : {
        *(.data*)
        . = ALIGN(8);
    } > FLASH

    .param_schema : {
        KEEP(*(.param_schema*))
        . = ALIGN(8);
    } > FLASH

    /DISCARD/ : {
        *(.bss*)
        *(COMMON)
        *(.ARM.exidx*)
        *(.ARM.extab*)
        *(.ARM.attributes)
        *(.eh_frame*)
        *(.comment)
        *(.note*)
        *(.debug*)
    }
}

PROVIDE(_code_size = _code_end - _code_start);
PROVIDE(_data_size = 0);
PROVIDE(_bss_size = 0);
