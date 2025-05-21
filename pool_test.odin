package opq

import "core:fmt"
import "core:log"
import "base:runtime"
import "core:strings"
import "core:sync"
import "core:time"
import "core:testing"
import "pq"

with_pool :: proc(t: ^testing.T, body: proc(t: ^testing.T, p: ^Pool)) {
	cfg := default_config(5)
    p, err := new_pool(&cfg)
    testing.expect(t, err == .None, fmt.tprintf("new_pool failed: %v", err))
    defer close_pool(p) // Ensure pool is closed
    body(t, p)
    // NOTE: close_pool only closes idle connections.
    // Active connections are expected to be released by the user.
    // If caller "leaks" a connection (acquires but doesn't release),
    // the pool will not close it, only be closed when the pool is closed.
}

@(test)
test_acquire_release :: proc(t: ^testing.T) {
    cfg := default_config(1)
    with_pool(t, proc(t: ^testing.T, p: ^Pool) {
        conn, err := acquire(p)
        testing.expect(t, err == .None, "acquire failed")
        testing.expect(t, conn != nil, "acquired conn is nil")
        release(p, conn)
        testing.expect(t, len(p.idle_conns) == 1, "idle_conns should have 1 after release")
    })
}
