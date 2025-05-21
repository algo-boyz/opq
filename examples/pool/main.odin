package main

import "core:fmt"
import "core:log"
import "../.."
import "../memtrack"

main :: proc() {
    tracker: memtrack.MemoryTracker
    context.allocator = memtrack.init(&tracker, context.allocator)  
    defer memtrack.terminate(&tracker)
    defer memtrack.panic_if_bad_frees_or_leaks(&tracker)

    context.logger = log.create_console_logger(opt = log.Options{.Level, .Terminal_Color, .Short_File_Path, .Line} | log.Full_Timestamp_Opts )
    defer log.destroy_console_logger(context.logger)
    
	cfg := opq.default_config()
    log.info(fmt.tprintf("default config: %v", cfg))
    p, err := opq.new_pool(&cfg)
        if err != nil {
        log.error(fmt.tprintf("new pool failed: %v", err))
        return
    }
    defer opq.close_pool(p) // Ensure pool is closed

    log.info("acquiring connection...")

    conn, err2 := opq.acquire(p)
    if err2 != nil {
        log.error(fmt.tprintf("acquire failed: %v", err2))
        return
    }
    if conn == nil {
        log.error("acquired conn is nil")
        return
    }
    log.info("acquired connection successfully")

    opq.release(p, conn)
    if len(p.idle_conns) != 1 {
        log.error("idle_conns should have 1 after release")
    }
    log.info("released connection successfully")
    log.info(fmt.tprintf("idle_conns count: %d", len(p.idle_conns)))
}