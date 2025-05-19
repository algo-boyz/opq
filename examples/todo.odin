package main

import "core:fmt"
import "core:log"
import "core:strings"
import "core:time"
import "memtrack"
import pq "../pq"
import "../"

DB_HOST     :: "localhost"
DB_PORT     :: "5432"
DB_USER     :: "postgres"
DB_PASS     :: "password"
DB_DBNAME   :: "war"
SSL_MODE    :: "disable"

Todo :: struct {
    id:          i64     `json:"id" db:"id"`,
    title:       string  `json:"title" db:"title"`,
    description: ^string `json:"description" db:"description"`,
    completed:   bool    `json:"completed" db:"completed"`,
    created_at:  time.Time `json:"created_at" db:"created_at"`,
    updated_at:  time.Time `json:"updated_at" db:"updated_at"`,
}

create_todo :: proc(conn: pq.Conn, todo: ^Todo) -> (id: i64, err: opq.Err) {
    query :: `
    INSERT INTO todos (title, description) 
    VALUES ($1, $2)
    RETURNING id;
    `
    desc: any = nil
    if todo.description != nil {
        desc = todo.description^
    }
    result: pq.Result
    result, err = opq.exec(conn, query, todo.title, desc)
    if err != .None {
        return -1, err
    }
    return opq.id_from_result(result)
}

get_todo :: proc(conn: pq.Conn, id: i64, todo_dest: ^Todo) -> opq.Err {
    query :: `
    SELECT id, title, description, completed, created_at, updated_at 
    FROM todos 
    WHERE id = $1;
    `
    return opq.query_row(conn, todo_dest, query, id)
}

update_todo :: proc(conn: pq.Conn, todo: ^Todo) -> (err: opq.Err) {
    query :: `
    UPDATE todos 
    SET title = $1, description = $2, completed = $3 
    WHERE id = $4;
    `
    result: pq.Result
    result, err = opq.exec(conn, query, todo.title, todo.description, todo.completed, todo.id)
    if err != .None {
        return err
    }
    return opq.ok_from_result(conn, result)
}

delete_todo :: proc(conn: pq.Conn, id: i64) -> (err: opq.Err) {
    query :: `
    DELETE FROM todos 
    WHERE id = $1;
    `
    return opq.del(conn, query, id)
}

main :: proc() {
    tracker: memtrack.MemoryTracker
    context.allocator = memtrack.init(&tracker, context.allocator)  
    defer memtrack.terminate(&tracker)
    defer memtrack.panic_if_bad_frees_or_leaks(&tracker)

    context.logger = log.create_console_logger(opt = log.Options{.Level, .Terminal_Color, .Short_File_Path, .Line} | log.Full_Timestamp_Opts )
    defer log.destroy_console_logger(context.logger)
    
    conn := opq.connect(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_DBNAME, SSL_MODE)
    defer pq.finish(conn)
    log.infof("Successfully connected to database: %s", DB_DBNAME)

    migration :: `
    CREATE TABLE IF NOT EXISTS todos (
        id          BIGSERIAL PRIMARY KEY,
        title       TEXT NOT NULL,
        description TEXT,
        completed   BOOLEAN NOT NULL DEFAULT FALSE,
        created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
    `
    create_err := opq.create_migration(conn, migration)
    if create_err != .None {
        log.errorf("Failed to ensure tables are created: %v", create_err)
        return
    }
    log.info("migration successful")

    new_todo: Todo
    new_todo.title = "Todo"
    desc_content := "Description from todo."
    new_todo.description = new(string)
    new_todo.description^ = strings.clone(desc_content)
    new_id, new_todo_err := create_todo(conn, &new_todo)
    
    if new_todo.description != nil {
        delete(new_todo.description^)
        free(new_todo.description)
    }
    if new_todo_err != .None {
        log.errorf("Failed to create todo: %v", new_todo_err)
        return
    }
    log.infof("Created new todo with ID: %d", new_id)

    fetched_todo: Todo
    get_err := get_todo(conn, new_id, &fetched_todo)
    if get_err != .None {
        if get_err == .Not_Found {
            log.infof("Todo with ID %d not found.", new_id)
        } else {
            log.errorf("Failed to get todo with ID %d: Error %v", new_id, get_err)
        }
    } else {
        log.infof("Fetched Todo: ID=%d, Title='%s'", fetched_todo.id, fetched_todo.title)
        if fetched_todo.description != nil {
            log.infof("  Description: '%s'", fetched_todo.description^)
        } else {
            log.info("  Description: NULL")
        }
        log.infof("  Completed: %v", fetched_todo.completed)
        created_at, ok := time.time_to_rfc3339(fetched_todo.created_at)
        if !ok {
            log.errorf("Failed to format created_at: %v", ok)
            return
        }
        defer delete(created_at)
        log.infof("  Created At: %s", created_at)
        if fetched_todo.description != nil {
            free(fetched_todo.description)
        }
    }

    up_todo: Todo
    up_todo.id = new_id
    up_todo.title = "Updated Todo"
    up_todo.description = new(string)
    up_todo.description^ = strings.clone("Updated description")
    up_todo.completed = true
    update_err := update_todo(conn, &up_todo)
    if up_todo.description != nil {
        delete(up_todo.description^)
        free(up_todo.description)
    }
    if update_err != .None {
        log.errorf("Failed to update todo: %v", update_err)
        return
    }
    log.infof("Updated todo with ID: %d", new_id)

    delete_todo_err := delete_todo(conn, new_id)
    if delete_todo_err != .None {
        log.errorf("Failed to delete todo with ID %d: Error %v", new_id, delete_todo_err)
    } else {
        log.infof("Deleted todo with ID: %d", new_id)
    }
    log.info("Example finished.")
}