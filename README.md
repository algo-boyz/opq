
<img src="opq.png" alt="opq" width="960">

<p align="center">opq is a convenience wrapper around libpq to make working with psql a bliss</p>

Example use:
```odin
Todo :: struct {
    id:          i64     `json:"id" db:"id"`,
    title:       string  `json:"title" db:"title"`,
    description: ^string `json:"description" db:"description"`,
    completed:   bool    `json:"completed" db:"completed"`,
    created_at:  time.Time `json:"created_at" db:"created_at"`,
    updated_at:  time.Time `json:"updated_at" db:"updated_at"`,
}

migrate :: proc(conn: pq.Conn) -> (err: opq.Err) {
    migration :: `
    CREATE TABLE IF NOT EXISTS todos (
        id          BIGSERIAL PRIMARY KEY,
        title       TEXT NOT NULL,
        description TEXT,
        completed   BOOLEAN NOT NULL DEFAULT FALSE,
        created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );`
    if err = opq.create_migration(conn, migration); err != .None {
        log.errorf("Failed to ensure tables are created: %v", err)
        return err
    }
    log.info("migration successful")
    return .None
}

create_todo :: proc(conn: pq.Conn, todo: ^Todo) -> (id: i64, err: opq.Err) {
    query :: `
    INSERT INTO todos (title, description) 
    VALUES ($1, $2)
    RETURNING id;`
    desc: any = nil
    if todo.description != nil {
        desc = todo.description^
    }
    result: pq.Result
    result, err = opq.exec(conn, query, todo.title, desc)
    if err != .None {
        return -1, err
    }
    defer pq.clear(result)
    return opq.id_from_result(result)
}

get_todo :: proc(conn: pq.Conn, id: i64, dest: ^Todo) -> opq.Err {
    query :: `
    SELECT id, title, description, completed, created_at, updated_at 
    FROM todos 
    WHERE id = $1;`
    return opq.query_row(conn, dest, query, id)
}

get_todos :: proc(conn: pq.Conn, dest: ^[dynamic]Todo) -> opq.Err {
    query :: `
    SELECT id, title, description, completed, created_at, updated_at 
    FROM todos`
    return opq.query_rows(conn, dest, query)
}

update_todo :: proc(conn: pq.Conn, todo: ^Todo) -> (err: opq.Err) {
    query :: `
    UPDATE todos 
    SET title = $1, description = $2, completed = $3 
    WHERE id = $4;`
    result: pq.Result
    result, err = opq.exec(conn, query, todo.title, todo.description, todo.completed, todo.id)
    if err != .None {
        return err
    }
    defer pq.clear(result)
    return opq.ok_from_result(conn, result)
}

delete_todo :: proc(conn: pq.Conn, id: i64) -> (err: opq.Err) {
    query :: `
    DELETE FROM todos 
    WHERE id = $1;`
    return opq.del(conn, query, id)
}
```

<p align="center">𒉭 𐱅𐰇𐰼𐰰 𖣐</p>