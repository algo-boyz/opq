package main

import "core:fmt"
import "core:log"
import "core:strings"
import "core:time"
import "../.."
import pq "../../pq"
import "../memtrack"

DB_HOST     :: "localhost"
DB_PORT     :: "5432"
DB_USER     :: "postgres"
DB_PASS     :: "password"
DB_DBNAME   :: "war"
SSL_MODE    :: "disable"

main :: proc() {
    tracker: memtrack.MemoryTracker
    context.allocator = memtrack.init(&tracker, context.allocator)  
    defer memtrack.terminate(&tracker)
    defer memtrack.panic_if_bad_frees_or_leaks(&tracker)

    context.logger = log.create_console_logger(opt = log.Options{.Level, .Terminal_Color, .Short_File_Path, .Line} | log.Full_Timestamp_Opts )
    defer log.destroy_console_logger(context.logger)
    
    conn, err := opq.connect(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_DBNAME, SSL_MODE)
    if err != .None {
        log.errorf("Failed to connect to database: %v", err)
        return
    }
    defer pq.finish(conn)
    log.infof("Successfully connected to database: %s", DB_DBNAME)

    if err = migrate(conn); err != .None {
        log.errorf("Failed to ensure tables are created: %v", err)
        return
    }
    new_todo: Todo
    new_todo.title = "Todo"
    desc_content := "Description from todo."
    new_todo.description = new(string)
    new_todo.description^ = strings.clone(desc_content)
    new_id: i64
    new_id, err = create_todo(conn, &new_todo)
    if new_todo.description != nil {
        delete(new_todo.description^)
        free(new_todo.description)
    }
    if err != .None {
        log.errorf("Failed to create todo: %v", err)
        return
    }
    log.infof("Created new todo with ID: %d", new_id)

    fetched_todo: Todo
    if err = get_todo(conn, new_id, &fetched_todo); err != .None {
        if err == .Not_Found {
            log.infof("Todo with ID %d not found.", new_id)
        } else {
            log.errorf("Failed to get todo with ID %d: Error %v", new_id, err)
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
            delete(fetched_todo.description^)
            free(fetched_todo.description)
        }
    }
    up_todo: Todo
    up_todo.id = new_id
    up_todo.title = "Updated Todo"
    up_todo.description = new(string)
    up_todo.description^ = strings.clone("Updated description")
    up_todo.completed = true
    if err = update_todo(conn, &up_todo); err != .None {
        log.errorf("Failed to update todo: %v", err)
        return
    }
    if up_todo.description != nil {
        delete(up_todo.description^)
        free(up_todo.description)
    }
    log.infof("Updated todo with ID: %d", up_todo.id)

    all_todos: [dynamic]Todo
    if err = get_todos(conn, &all_todos); err != .None {
        log.errorf("Failed to get all todos: %v", err)
        return
    }
    log.info("All Todos:")
    for i := 0; i < len(all_todos); i+=1 {
        todo := all_todos[i]
        log.infof("  ID=%d, Title='%s'", todo.id, todo.title)
        if todo.description != nil {
            log.infof("    Description: '%s'", todo.description^)
        } else {
            log.info("    Description: NULL")
        }
        log.infof("    Completed: %v", todo.completed)
        created_at, ok := time.time_to_rfc3339(todo.created_at)
        if !ok {
            log.errorf("Failed to format created_at: %v", ok)
            return
        }
        defer delete(created_at)
        log.infof("    Created At: %s", created_at)
        if todo.description != nil {
            delete(todo.description^)
            free(todo.description)
        }
        if err = delete_todo(conn, todo.id); err != .None {
            log.errorf("Failed to delete todo with ID %d: Error %v", todo.id, err)
        } else {
            log.infof("Deleted todo with ID: %d", todo.id)
        }
    }
    if len(all_todos) > 0 {
        delete(all_todos)
    }
    log.info("Example finished.")
}