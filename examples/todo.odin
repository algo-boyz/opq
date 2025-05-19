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

get_todo :: proc(conn: pq.Conn, id: i64, todo_dest: ^Todo) -> opq.Err {
    query :: `
    SELECT id, title, description, completed, created_at, updated_at 
    FROM todos 
    WHERE id = $1;
    `
    return opq.query_row(conn, todo_dest, query, id)
}


create_todo :: proc(conn: pq.Conn, todo_data: ^Todo) -> (id: i64, err: opq.Err) {
    query :: `
    INSERT INTO todos (title, description) 
    VALUES ($1, $2)
    RETURNING id;
    `
    desc_param: any = nil
    if todo_data.description != nil {
        desc_param = todo_data.description^
    }
    result, exec_err := opq.exec(conn, query, todo_data.title, desc_param)
    if exec_err != .None {
        return -1, exec_err
    }
    return opq.id_from_result(result)
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
    create_err := opq.new_migration(conn, migration)
    if create_err != .None {
        log.errorf("Failed to ensure tables are created: %v", create_err)
        return
    }

    new_todo_data: Todo
    new_todo_data.title = "Todo"
    desc_content := "Description from todo."
    new_todo_data.description = new(string)
    new_todo_data.description^ = strings.clone(desc_content)
    new_id, create_todo_err := create_todo(conn, &new_todo_data)
    
    if new_todo_data.description != nil {
        delete(new_todo_data.description^)
        free(new_todo_data.description)
    }
    if create_todo_err != .None {
        log.errorf("Failed to create todo: %v", create_todo_err)
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
        // log.infof("  Created At: %s", time.format_iso8601(fetched_todo.created_at)) // Example formatting
        delete(fetched_todo.title)
        if fetched_todo.description != nil {
            delete(fetched_todo.description^)
            free(fetched_todo.description)
        }
    }
    log.info("Example finished.")
}