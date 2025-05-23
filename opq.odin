package opq
import "base:runtime"
import "core:fmt"
import "core:log"
import "core:os"
import "core:strings"
import "core:strconv"
import "core:time"
import "core:reflect"
import "pq"

Err :: enum i32 {
	None = 0,
    Pool_Closed = 1,
    Acquire_Timeout = 2,
    Connection_Failed = 3,
	Migration_Failed = 4,
	Query_Failed = 5,
	Result_Error = 6,
	Bad_Parameter = 7,
	Parsing_Failed = 8,
	Not_Found = 9,
	Column_Not_Found = 10,
	Is_Nil = 11, // Value is unexpectedly NULL in DB for a non-pointer field
	Allocation_Error = 12,
	Time_Format_Error = 13,
	Unsupported_Type = 14,
    Precondition_Not_Met = 15,
}

to_string :: proc(c_str: cstring) -> string {
    if c_str == nil {
        return ""
    }
    s, err := strings.clone_from_cstring(c_str)
    if err != nil {
        log.errorf("Failed to clone_from_cstring: %v", err)
        return ""
    }
    delete(c_str) // Free the C string after cloning
    return s
}

pq_cstr_with_len :: proc(data_ptr: [^]byte, length: i32) -> string {
	if data_ptr == nil || length <= 0 {
		return ""
	}
	return string(data_ptr[:length])
}

get_tag_val :: proc(tag: string) -> (tag_value: string, has_db_tag: bool) {
    tags := strings.split(tag, " ")
    defer delete(tags)
    for tag in tags {
        if strings.has_prefix(tag, "db:") {
            // Remove "db:" prefix and trailing quote
            return tag[4:len(tag)-1], true
        }
    }
    return "", false
}

// to_pq_param_single converts a value to pq parameter parts.
to_pq_param :: proc(val: any) -> (p_val: cstring, p_len: i32, p_fmt: pq.Format, err_code: Err) {
    if val == nil {
        return nil, 0, .Text, .None
    }
    raw_type_info := type_info_of(val.id)
	ti := runtime.type_info_base(raw_type_info)
	a := any{val.data, ti.id}
	switch v_typ in a {
    case string:
        v_str := v_typ
        p_val = strings.clone_to_cstring(v_str)
        if p_val == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for string")
            return nil, 0, .Text, .Allocation_Error
        }
        return p_val, i32(len(v_str)), .Text, .None
    case ^string:
        v_ptr := v_typ
        if v_ptr == nil {
            return nil, 0, .Text, .None
        }
        v_str := v_ptr^
        c_str := strings.clone_to_cstring(v_str)
        if c_str == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^string content")
            return nil, 0, .Text, .Allocation_Error
        }
        return c_str, i32(len(v_str)), .Text, .None
    case f64:
        v_str := fmt.tprintf("%f", v_typ)
        p_val = strings.clone_to_cstring(v_str)
        length := len(v_str)
        if p_val == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for f64")
            return nil, 0, .Text, .Allocation_Error 
        }
        return p_val, i32(length), .Text, .None
    case ^f64:
        v_ptr := v_typ
        if v_ptr == nil {
            return nil, 0, .Text, .None
        }
        v_f64 := v_ptr^
        v_str := fmt.tprintf("%f", v_typ)
        length := len(v_str)
        c_str := strings.clone_to_cstring(v_str)
        if c_str == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^f64 content")
            return nil, 0, .Text, .Allocation_Error
        }
        return c_str, i32(length), .Text, .None
    case i64:
        v_str := fmt.tprintf("%d", v_typ)
        p_val = strings.clone_to_cstring(v_str)
        length := len(v_str)
        if p_val == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for i64")
            return nil, 0, .Text, .Allocation_Error 
        }
        return p_val, i32(length), .Text, .None
    case ^i64:
        v_ptr := v_typ
        if v_ptr == nil {
            return nil, 0, .Text, .None
        }
        v_i64 := v_ptr^
        v_str := fmt.tprintf("%d", v_typ)
        length := len(v_str)
        c_str := strings.clone_to_cstring(v_str)
        if c_str == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^i64 content")
            return nil, 0, .Text, .Allocation_Error
        }
        return c_str, i32(length), .Text, .None
    case bool:
        val_str := "f"
        if val.(bool) { val_str = "t" }
        c_str := strings.clone_to_cstring(val_str)
        if c_str == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for bool")
            return nil, 0, .Text, .Allocation_Error 
        }
        return c_str, 1, .Text, .None
    case ^bool:
        v_ptr := v_typ
        if v_ptr == nil {
            return nil, 0, .Text, .None
        }
        v_bool := v_ptr^
        val_str := "f"
        if v_bool { val_str = "t" }
        c_str := strings.clone_to_cstring(val_str)
        if c_str == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^bool content")
            return nil, 0, .Text, .Allocation_Error 
        }
        return c_str, 1, .Text, .None
    case []byte:
        v_bytes := v_typ
        if v_bytes == nil {
            return nil, 0, .Text, .None
        }
        length := len(v_bytes)
        c_str := strings.clone_to_cstring(string(v_bytes))
        if c_str == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for []byte")
            return nil, 0, .Text, .Allocation_Error 
        }
        return c_str, i32(length), .Text, .None
    case ^[]byte:
        v_ptr := v_typ
        if v_ptr == nil {
            return nil, 0, .Text, .None
        }
        v_bytes := v_ptr^
        length := len(v_bytes)
        c_str := strings.clone_to_cstring(string(v_bytes))
        if c_str == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^[]byte content")
            return nil, 0, .Text, .Allocation_Error 
        }
        return c_str, i32(length), .Text, .None
    case time.Time:
        t := a.(time.Time)
        val, ok := time.time_to_rfc3339(t)
        if !ok {
            log.errorf("to_pq_param: Failed to convert time.Time to string")
            return nil, 0, .Text, .Allocation_Error
        }
        c_str := strings.clone_to_cstring(val)
        length := len(val)
        delete(val)
        if c_str == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for time")
            return nil, 0, .Text, .Allocation_Error 
        }
        return c_str, i32(length), .Text, .None
    case:
        log.errorf("to_pq_param: Unsupported type: %v for value %v", v_typ, val)
    }
    return nil, 0, .Text, .Unsupported_Type
}

begin_tx :: proc(conn: pq.Conn) -> Err {
    res, err := exec(conn, "BEGIN")
    if err != .None {
        log.errorf("opq.begin: Failed to begin transaction: %v", err)
        return err
    }
    defer pq.clear(res) // Ensure res is cleared.
    return ok_from_result(conn, res)
}

commit_tx :: proc(conn: pq.Conn) -> Err {
    res, err := exec(conn, "COMMIT")
    if err != .None {
        log.errorf("opq.commit: Failed to commit transaction: %v", err)
        return err
    }
    defer pq.clear(res) // Ensure res is cleared.
    return ok_from_result(conn, res)
}

rollback_tx :: proc(conn: pq.Conn) -> Err {
    res, err := exec(conn, "ROLLBACK")
    if err != .None {
        log.errorf("opq.rollback: Failed to rollback transaction: %v", err)
        return err
    }
    defer pq.clear(res) // Ensure res is cleared.
    return ok_from_result(conn, res)
}

// with_tx wraps a transaction around the provided body proc
with_tx :: proc(conn: pq.Conn, body: proc(tx_conn: pq.Conn) -> Err) -> (err: Err) {
    if err = begin_tx(conn); err != .None {
        return err
    }
    // Pass the same connection, as it's now in a transaction state
    if err = body(conn); err != .None {
        // Attempt to rollback, but prioritize returning the original body error
        if roll_err := rollback_tx(conn); roll_err != .None {
            log.errorf("opq.with_transaction: Failed to rollback after error: %v (original error: %v)", roll_err, err)
        }
        return err
    }
    if err = commit_tx(conn); err != .None {
        // Attempt to rollback if commit fails
        if roll_err := rollback_tx(conn); roll_err != .None {
            log.errorf("opq.with_transaction: Failed to rollback after commit failure: %v (commit error: %v)", roll_err, err)
        }
        return err
    }
    return .None
}

// new_migration sets up the necessary database table if it doesn't exist.
create_migration :: proc(conn: pq.Conn, query: string) -> (err: Err) {
    res, create_err := exec(conn, query)
    if create_err != .None {
        log.errorf("Failed to execute migration query: %v", create_err)
        return .Migration_Failed
    }
    defer pq.clear(res) // Ensure res is cleared.
    return ok_from_result(conn, res)
}

// delete removes a row from the database.
del :: proc(conn: pq.Conn, query: string, arg: any) -> (err: Err) {
	res, exec_err := exec(conn, query, arg)
	if exec_err != .None {
		return exec_err
	}
    if res == nil {
        log.error("opq.del: exec returned nil res without error.")
        return .Query_Failed
    }
	defer pq.clear(res)
	return ok_from_result(conn, res)
}

// exec is an Odin-friendly wrapper for pq.exec_params
// It converts variadic Odin arguments to C params and manages their memory.
// The caller is responsible to clear the returned result.
exec :: proc(conn: pq.Conn, query: string, args: ..any) -> (res: pq.Result, err: Err) {
    c_str := strings.clone_to_cstring(query)
    if c_str == nil {
        log.error("notify_channel: Failed to allocate C string for query.")
        return nil, .Allocation_Error
    }
    defer delete(c_str)
    n_params := len(args)
    if n_params == 0 {
        res = pq.exec(conn, c_str)
        if res == nil {
            err_msg := to_string(pq.error_message(conn))
            log.error(err_msg)
            delete(err_msg)
            return nil, .Query_Failed
        }
        return res, .None 
    }
    // Slices themselves are on stack; arrays are heap-allocated with make
    param_values_c  := make([][^]byte, n_params)
    param_lengths_c := make([]i32,   n_params)
    param_formats_c := make([]pq.Format, n_params)
    defer {
		for i := 0; i < n_params; i += 1 {
			if param_values_c[i] != nil {
				free(param_values_c[i]) // Free cstrings allocated by to_pq_param
			}
		}
        delete(param_values_c)
        delete(param_lengths_c)
        delete(param_formats_c)
    }
    for arg, i in args {
        c_val, length, format, conv_err := to_pq_param(arg)
        if conv_err != .None {
            log.errorf("Failed to convert arg #%d to C param: %v", i, conv_err)
            return nil, conv_err
        }
        param_values_c[i]  = cast([^]byte)c_val // c_val can be nil for SQL NULL
        param_lengths_c[i] = length
        param_formats_c[i] = format
    }
    res = pq.exec_params(
        conn,
        c_str,
        i32(n_params),
        nil,                 // param_types (OIDs) - let server infer. Can be specified for more control.
        &param_values_c[0],  // Pointer to the first element
        &param_lengths_c[0],
        &param_formats_c[0],
        .Text,               // result_format (can be configurable, .Binary for performance with some types)
    )
    return res, .None
}

// query_rows executes a query that returns multiple rows and scans them
// into the `dest` slice. `dest` should be a pointer to a slice of structs.
// Manages pq.Result clearing.
query_rows :: proc(conn: pq.Conn, dest_slice_ptr: ^[dynamic]$T, query: string, args: ..any) -> (err: Err) {
	res: pq.Result
    res, err = exec(conn, query, ..args)
	if err != .None {
		return err
	}
	if res == nil {
		log.error("opq.query_rows: exec returned nil res unexpectedly (exec_err was .None).")
		return .Query_Failed
	}
	defer pq.clear(res)
    
    num_tuples := pq.n_tuples(res)
	if num_tuples == 0 {
		return .None
	}
	for row_idx: i32 = 0; row_idx < num_tuples; row_idx += 1 {
		current_row_item: T 
		if err = scan_row(res, &current_row_item, row_idx); err != .None {
			log.errorf("opq.query_rows: Failed to scan data for row #%d. Error: %v. Query: %s", row_idx, err, query)
			return err
		}
		append(dest_slice_ptr, current_row_item)
	}
	return .None
}

// query_row executes a query expected to return one row (or zero for Not_Found)
// and scan it into the `dest` struct.
query_row :: proc(conn: pq.Conn, dest_struct_ptr: ^$T, query: string, args: ..any) -> Err {
	res, err := exec(conn, query, ..args)
	if err != .None {
		return err
	}
	defer pq.clear(res)
    
	num_tuples := pq.n_tuples(res)
	if num_tuples == 0 {
		return .Not_Found
	}
	if num_tuples > 1 {
		log.warnf("opq.query_row: Expected 1 row, got %d. Scanning the first row. Query: %s", num_tuples, query)
		// Depending on strictness, one might return an error here.
	}
	
	return scan_row(res, dest_struct_ptr, 0) // Scan the first row (index 0)
}

scan_row :: proc(res: pq.Result, dest: ^$T, row_idx: i32) -> (err: Err) {
    ti := runtime.type_info_base(type_info_of(T))
	s, ok := ti.variant.(runtime.Type_Info_Struct)
    if !ok {
        return .Bad_Parameter
    }
    for i:i32; i < s.field_count; i += 1 {
        tag_val, has_tag := get_tag_val(s.tags[i])
        if !has_tag {
            log.debug("No db tag found for field", i, "skipping")
            continue
        }
        tag_val_cstr := strings.clone_to_cstring(tag_val)
        if tag_val_cstr == nil {
            log.errorf("Failed to convert db_tag '%s' to cstring for field '%s'", tag_val, tag_val)
            return .Allocation_Error 
        }
        col_idx := pq.f_number(res, tag_val_cstr)
        delete(tag_val_cstr)
        if col_idx < 0 {
            return .Column_Not_Found 
        }
        is_null   := pq.get_is_null(res, row_idx, col_idx)
        if is_null {
            return .Is_Nil
        }
        val_ptr   := pq.get_value(res, row_idx, col_idx) 
        val_len   := pq.get_length(res, row_idx, col_idx)
        val := pq_cstr_with_len(val_ptr, val_len)
        if err = scan_field(val, dest, s, i); err != .None {
            log.errorf("Failed to scan field %s: %v", s.names[i], err)
            return err
        }
    }
    return .None
}

scan_field :: proc(val: string, dest: ^$T, s: runtime.Type_Info_Struct, i:i32) -> Err {
    fields := reflect.struct_field_types(T)
    f := reflect.struct_field_value_by_name(dest^, s.names[i])
    switch fields[i].id {
    case i32:
        new_int, ok := strconv.parse_int(val)
        if !ok {
            log.errorf("Failed to parse i32 from string: '%s'", val)
            return .Parsing_Failed
        }
        (^i32)(f.data)^ = i32(new_int)
    case ^i32:
        ptr := new(i32)
        ptr^ = 0
        new_int, ok := strconv.parse_int(val)
        if !ok {
            log.errorf("Failed to parse i32 from string: '%s'", val)
            return .Parsing_Failed
        }
        ptr^ = i32(new_int)
        (^(^i32))(f.data)^ = ptr
    case i64:
        new_i64, ok := strconv.parse_i64(val)
        if !ok {
            log.errorf("Failed to parse i64 from string: '%s'", val)
            return .Parsing_Failed
        }
        (^i64)(f.data)^ = new_i64
    case ^i64:
        ptr := new(i64)
        ptr^ = 0
        new_i64, ok := strconv.parse_i64(val)
        if !ok {
            log.errorf("Failed to parse i64 from string: '%s'", val)
            return .Parsing_Failed
        }
        ptr^ = new_i64
        (^(^i64))(f.data)^ = ptr
    case f64:
        new_f64, ok := strconv.parse_f64(val)
        if !ok {
            log.errorf("Failed to parse f64 from string: '%s'", val)
            return .Parsing_Failed
        }
        (^f64)(f.data)^ = new_f64
    case ^f64:
        ptr := new(f64)
        ptr^ = 0.0
        new_f64, ok := strconv.parse_f64(val)
        if !ok {
            log.errorf("Failed to parse f64 from string: '%s'", val)
            return .Parsing_Failed
        }
        ptr^ = new_f64
        (^(^f64))(f.data)^ = ptr
    case string:
        (^string)(f.data)^ = val
    case ^string:
        ptr := new(string)
        ptr^ = strings.clone(val)
        (^(^string))(f.data)^ = ptr
    case bool:
        if val == "t" {
            (^bool)(f.data)^ = true
        } else if val == "f" {
            (^bool)(f.data)^ = false
        } else {
            log.errorf("Failed to parse bool from string: '%s'", val)
            return .Parsing_Failed
        }
    case ^bool:
        ptr := new(bool)
        ptr^ = false
        if val == "t" {
            ptr^ = true
        } else if val == "f" {
            ptr^ = false
        } else {
            log.errorf("Failed to parse bool from string: '%s'", val)
            return .Parsing_Failed
        }
        (^(^bool))(f.data)^ = ptr
    case time.Time:
        t, consumed := time.rfc3339_to_time_utc(val)
        if consumed >= len(val) {
            log.errorf("Failed to parse time from string: '%s'", val)
            return .Time_Format_Error
        }
        (^time.Time)(f.data)^ = t
    case []byte:
        // UUIDs: PostgreSQL has a native UUID type. might want to map this to [16]byte or a string, with appropriate parsing/formatting.
        // JSON/JSONB: These could be mapped to string or []byte (raw JSON)
        // pq.get_value returns [^]byte, and pq.get_length gives its length.
        // will need to clone this data into an Odin []byte.
        // Note: pq.get_value result is not null-terminated if it's binary.
        // The 'val' string passed to scan_field might need to be [^]byte and length if dealing with raw binary from pq.
        // For now, assuming 'val' is a string representation if it's text format.
        // If pq.get_value was directly used with binary format, we'd copy bytes.
        // For text format bytea (e.g., \xDEADBEEF), unescaping is needed here.
        // pq.unescape_bytea would be used if bytea was fetched as text.
        log.warnf("scan_field: []byte from string representation is tricky; recommend binary format retrieval for bytea.")
        // or a hex string like "\x..." ???
        // unescaped_bytes, unescaped_len := pq.unescape_bytea(strings.clone_to_cstring(val), nil)
        // if unescaped_bytes != nil {
        //    (^[dynamic]byte)(f.data)^ = unescaped_bytes[:unescaped_len]
        //    pq.free_mem(unescaped_bytes)
        // } else { return .Parsing_Failed }
    case ^[]byte:
        // TODO
    case:
        log.errorf("Unsupported type for field %s: %v", s.names[i], s.types[i])
        // return .Unsupported_Type
    }
    return .None
}

// id_from_result extracts the ID from a res object.
// proc just focuses on extracting data from an assumed valid result
id_from_result :: proc(res: pq.Result) -> (id: i64, err: Err) {
	if res == nil {
		log.error("opq.id_from_result: Received nil res.")
		return -1, .Is_Nil
	}
	if pq.n_tuples(res) != 1 {
		log.errorf("opq.id_from_result: Expected 1 tuple (row) for ID, got %d.", pq.n_tuples(res))
		return -1, .Result_Error
	}
	if pq.n_fields(res) != 1 {
		log.errorf("opq.id_from_result: Expected 1 field (column) for ID, got %d.", pq.n_fields(res))
		return -1, .Result_Error
	}

	if pq.get_is_null(res, 0, 0) {
		log.error("opq.id_from_result: RETURNING id value is NULL.")
		return -1, .Is_Nil
	}

	id_val_ptr := pq.get_value(res, 0, 0)
	id_val_len := pq.get_length(res, 0, 0)

	id_odin_str := pq_cstr_with_len(id_val_ptr, id_val_len)

	new_id, ok := strconv.parse_i64(id_odin_str)
	if !ok {
		log.errorf("opq.id_from_result: Failed to parse RETURNING id from string: '%s'", id_odin_str)
		return -1, .Parsing_Failed
	}
	return new_id, .None
}

// ok_from_result checks if a command res (no tuples expected, or tuples are fine) is okay.
// Does not clear the result; caller is responsible.
ok_from_result :: proc(conn: pq.Conn, res: pq.Result) -> (err: Err) {
	if res == nil {
		// This indicates a problem before even getting a res object,
		// likely a connection issue
		err_msg := to_string(pq.error_message(conn))
		log.errorf("opq.ok_from_result: Received nil res. PQ conn error: %s", err_msg)
		delete(err_msg)
		return .Is_Nil
	}

	status := pq.result_status(res)
	// .Tuples_OK is fine for commands that might return info (e.g. RETURNING)
	// .Command_OK is for commands that don't return rows (INSERT, UPDATE, DELETE without RETURNING)
	// .Single_Tuple for specific single row returns, also acceptable if data extraction is separate.
	if status != .Command_OK && status != .Tuples_OK && status != .Single_Tuple {
		err_msg := to_string(pq.result_error_message(res)) // Error from res
		log.errorf("opq.ok_from_result: Command failed. PQ Status: %v. Message: '%s'", status, err_msg)
		delete(err_msg)
		return .Result_Error
	}
	return .None
}