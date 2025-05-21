package opq

import "core:fmt"
import "core:log"
import "base:runtime"
import "core:strings"
import "core:sync"
import "core:time"
import "core:testing"
import "pq"
import "core:slice"

PoolConfig :: struct {
    host, port, user, pass, db_name, ssl_mode: string,
    max_open_conns:    int,      
    min_idle_conns:    int,      
    max_conn_lifetime: time.Duration,
    max_idle_time:     time.Duration,
    acquire_timeout:   time.Duration,
}

default_config :: proc() -> PoolConfig {
    return PoolConfig{
        host           = "localhost",
        port           = "5432",
        user           = "postgres",
        pass           = "password",
        db_name        = "war",
        ssl_mode       = "disable",
        max_open_conns = 1,
        acquire_timeout = 3 * time.Second, 
        max_conn_lifetime = 0, 
        max_idle_time    = 0, 
    }
}

pooledConn :: struct {
    conn:       pq.Conn,
    created_at: time.Time,   
    last_used_at: time.Time, 
}

Pool :: struct {
    cfg:               ^PoolConfig,
    mu:                ^sync.Mutex,        
    idle_conns:        [dynamic]pooledConn,
    num_open:          int,                
    cond:              sync.Cond,          
    closed:            bool,               
}

connect :: proc(host, port, user, pass, db_name, ssl_mode: string) -> (conn: pq.Conn, err: Err) {
    conn_str := fmt.tprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
            host, port, user, pass, db_name, ssl_mode)
    conn_cstr := strings.clone_to_cstring(conn_str)
    if conn_cstr == nil {
        log.error("opq.connect: Failed to allocate C string for connection.")
        return nil, .Allocation_Error
    }
    defer delete(conn_cstr)

    conn = pq.connectdb(conn_cstr)
    if conn == nil {
        log.error("opq.connect: Connection failed. pq.connectdb returned nil.")
        return nil, .Connection_Failed
    }
    if pq.status(conn) == .Bad {
        cerr := pq.error_message(conn)
        err_msg := ""
        if cerr != nil {
             err_msg = strings.clone_from_cstring(cerr)
        }
        log.errorf("opq.connect: Connection failed. PQ Status: %v. Message: %s", pq.status(conn), err_msg)
        if err_msg != "" { delete(err_msg) }
        pq.finish(conn) // Close bad connection
        return nil, .Connection_Failed
    }
    return conn, .None
}

new_pool :: proc(cfg: ^PoolConfig) -> (^Pool, Err) {
    if cfg.max_open_conns <= 0 {
        cfg.max_open_conns = 10 
    }
    if cfg.acquire_timeout <= 0 {
        cfg.acquire_timeout = 30 * time.Second
    }
    pool := new(Pool)
    pool.cfg = cfg
    pool.mu = new(sync.Mutex)
    pool.idle_conns = make([dynamic]pooledConn, 0, cfg.max_open_conns)

    return pool, .None
}

acquire :: proc(p: ^Pool) -> (conn: pq.Conn, err: Err) {
    sync.mutex_lock(p.mu) 
    deadline_ns := time.now()._nsec + i64(p.cfg.acquire_timeout)
    for { 
        if p.closed {
            sync.mutex_unlock(p.mu)
            log.warn("opq.acquire: Pool is closed.")
            return nil, .Pool_Closed
        }
        // 1. Try to get an existing idle connection
        if len(p.idle_conns) > 0 {
            // Get the first idle connection
            pc := p.idle_conns[0]
            ordered_remove(&p.idle_conns, 0) 
            log.infof("opq.acquire: Popped idle conn %p. created_at: %v, last_used_at: %v", pc.conn, pc.created_at, pc.last_used_at)

            if p.cfg.max_conn_lifetime > 0 && time.now()._nsec > time.time_add(pc.created_at, p.cfg.max_conn_lifetime)._nsec {
                log.infof("opq.acquire: Idle connection %p exceeded max_conn_lifetime. Closing.", pc.conn)
                pq.finish(pc.conn) 
                p.num_open -= 1
                sync.cond_signal(&p.cond) 
                continue 
            }
            if p.cfg.max_idle_time > 0 && time.now()._nsec > time.time_add(pc.last_used_at, p.cfg.max_idle_time)._nsec {
                log.infof("opq.acquire: Idle connection %p exceeded max_idle_time. Closing.", pc.conn)
                pq.finish(pc.conn)
                p.num_open -= 1
                sync.cond_signal(&p.cond)
                continue
            }
            // Health check
            if pc.conn == nil || pq.status(pc.conn) == .Bad { 
                log.warnf("opq.acquire: Stale/bad connection %p found in idle queue. Discarding.", pc.conn)
                if pc.conn != nil { // Only finish if it's not already nil
                    pq.finish(pc.conn)
                }
                p.num_open -= 1
                sync.cond_signal(&p.cond)
                continue
            }
            log.infof("opq.acquire: Acquired idle connection %p. Open: %d, Idle: %d", pc.conn, p.num_open, len(p.idle_conns))
            sync.mutex_unlock(p.mu)
            return pc.conn, .None
        }

        // 2. If no idle connections, try to open a new one if allowed
        if p.num_open < p.cfg.max_open_conns {
            p.num_open += 1 // Increment before unlock to reflect intent
            
            sync.mutex_unlock(p.mu) 
            log.infof("opq.acquire: Creating new connection. Current open (target): %d (max: %d)", p.num_open, p.cfg.max_open_conns)
            new_raw_conn, connect_err_code := connect(p.cfg.host, p.cfg.port, p.cfg.user, p.cfg.pass, p.cfg.db_name, p.cfg.ssl_mode)
            sync.mutex_lock(p.mu)

            if connect_err_code != .None || new_raw_conn == nil {
                log.errorf("opq.acquire: connect() failed. Code: %v, Conn: %p", connect_err_code, new_raw_conn)
                p.num_open -= 1      // Rollback increment
                sync.cond_signal(&p.cond) 
                sync.mutex_unlock(p.mu)
                return nil, .Connection_Failed
            }
            log.infof("opq.acquire: Created new connection %p. Total open now: %d", new_raw_conn, p.num_open)
            sync.mutex_unlock(p.mu)
            return new_raw_conn, .None 
        }

        // 3. If pool is full, wait.
        log.infof("opq.acquire: Max connections (%d) reached. Waiting for release...", p.cfg.max_open_conns)
        current_ns := time.now()._nsec
        if current_ns >= deadline_ns { 
            sync.mutex_unlock(p.mu)
            log.warn("opq.acquire: Timed out while waiting for an available connection (before cond_wait).")
            return nil, .Acquire_Timeout
        }
        remaining_timeout_ns := time.Duration(deadline_ns - current_ns)
        timed_out := sync.cond_wait_with_timeout(&p.cond, p.mu, remaining_timeout_ns)
        if timed_out {
            sync.mutex_unlock(p.mu)
            log.warn("opq.acquire: Timed out via cond_timed_wait.")
            return nil, .Acquire_Timeout
        }
        log.info("opq.acquire: Woke up from wait. Re-checking for available connection.")
    } 
}

release :: proc(p: ^Pool, conn_to_release: pq.Conn) {
    if conn_to_release == nil {
        log.warn("opq.release: Attempted to release a nil connection.")
        return
    }
    created_at := time.now() // TODO: ideally this is when connect() was called for this conn
    sync.mutex_lock(p.mu)
    defer sync.mutex_unlock(p.mu)

    if p.closed {
        pq.finish(conn_to_release)
        p.num_open -=1
        sync.cond_signal(&p.cond)
        return
    }

    if pq.status(conn_to_release) == .Bad {
        log.warnf("opq.release: Releasing a bad connection %p. Discarding.", conn_to_release)
        pq.finish(conn_to_release)
        p.num_open -= 1
        sync.cond_signal(&p.cond) 
        return
    }
    pc := pooledConn{
        conn=         conn_to_release,
        created_at=   created_at,
        last_used_at= time.now(),
    }
    // Check if idle queue is full
    if len(p.idle_conns) >= p.cfg.max_open_conns { 
        log.warnf("opq.release: Idle connection queue is full (len %d >= max_open %d). Discarding connection %p.",
            len(p.idle_conns), p.cfg.max_open_conns, pc.conn)
        pq.finish(pc.conn)
        // num_open should decrement because this connection is being closed, not idled
        p.num_open -= 1 
    } else {
        append(&p.idle_conns, pc)
        log.infof("opq.release: Connection %p released to idle. Open: %d, Idle: %d", pc.conn, p.num_open, len(p.idle_conns))
    }
    sync.cond_signal(&p.cond)
}

close_pool :: proc(p: ^Pool) {
    sync.mutex_lock(p.mu)
    if p.closed {
        sync.mutex_unlock(p.mu)
        return
    }
    p.closed = true
    log.infof("opq.close_pool: Closing pool. Draining %d idle connections.", len(p.idle_conns))
    // Close all idle connections
    for len(p.idle_conns) > 0 {
        pc := p.idle_conns[0]
        ordered_remove(&p.idle_conns, 0)
        
        if pc.conn != nil {
            pq.finish(pc.conn)
        }
        p.num_open -= 1 // Decrement each closed idle connection
    }
    // ensure num_open is not negative if logic error occurred
    if p.num_open < 0 {
        log.errorf("opq.close_pool: num_open became negative (%d). Resetting to 0.", p.num_open)
        p.num_open = 0
    }
    sync.broadcast(&p.cond) // Signal all waiting routines
    sync.mutex_unlock(p.mu)
    log.infof("opq.close_pool: Pool closed. Connections still marked 'in-use' if any: %d (should be released and discarded by app).", p.num_open)
	delete(p.idle_conns)
	free(p.mu)
	free(p)
}
