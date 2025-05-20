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
	Connection_Failed = 1,
	Migration_Failed = 2,
	Result_Error = 3,
	Bad_Parameter = 4,
	Query_Failed = 5,
	Parsing_Failed = 6,
	Not_Found = 7,
	Is_Nil = 8, // Value is unexpectedly NULL in DB for a non-pointer field
	Allocation_Error = 9,
	Time_Format_Error = 10,
	Column_Not_Found = 11,
	Unsupported_Type = 12,
    Precondition_Not_Met = 13,
}

to_string :: proc(c_str: cstring) -> string {
    if c_str == nil {
        return ""
    }
    // strings.clone_from_cstring allocates a new Odin string.
    // The caller of to_string is responsible for deleting this returned string.
    s, err := strings.clone_from_cstring(c_str)
    if err != nil {
        log.errorf("Failed to clone_from_cstring: %v", err)
        return ""
    }
    return s
}

pq_cstr_with_len :: proc(data_ptr: [^]byte, length: i32) -> string {
	if data_ptr == nil || length <= 0 {
		return ""
	}
	// This creates a new Odin string, copying the data.
	return string(data_ptr[:length])
}

get_tag_val :: proc(tag: string) -> (tag_value: string, has_db_tag: bool) {
    tags := strings.split(tag, " ")
    defer delete(tags) // Ensure tags slice is deleted after use
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
    if val == nil { // Handles untyped nil passed directly for SQL NULL
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
        v_ptr_str := v_typ
        if v_ptr_str == nil {
            return nil, 0, .Text, .None // SQL NULL
        }
        v_str := v_ptr_str^
        c_str := strings.clone_to_cstring(v_str)
        if c_str == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^string content")
            return nil, 0, .Text, .Allocation_Error
        }
        return c_str, i32(len(v_str)), .Text, .None
    case f64:
        odin_s := fmt.tprintf("%f", v_typ)
        p_val = strings.clone_to_cstring(odin_s)
        length := len(odin_s)
        if p_val == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for f64")
            return nil, 0, .Text, .Allocation_Error 
        }
        return p_val, i32(length), .Text, .None
    case ^f64:
        v_ptr_f64 := v_typ
        if v_ptr_f64 == nil {
            return nil, 0, .Text, .None // SQL NULL
        }
        v_f64 := v_ptr_f64^
        odin_s := fmt.tprintf("%f", v_typ)
        length := len(odin_s)
        c_str := strings.clone_to_cstring(odin_s)
        if c_str == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^f64 content")
            return nil, 0, .Text, .Allocation_Error
        }
        return c_str, i32(length), .Text, .None
    case i64:
        odin_s := fmt.tprintf("%d", v_typ)
        p_val = strings.clone_to_cstring(odin_s)
        length := len(odin_s)
        if p_val == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for i64")
            return nil, 0, .Text, .Allocation_Error 
        }
        return p_val, i32(length), .Text, .None
    case ^i64:
        v_ptr_i64 := v_typ
        if v_ptr_i64 == nil {
            return nil, 0, .Text, .None // SQL NULL
        }
        v_i64 := v_ptr_i64^
        odin_s := fmt.tprintf("%d", v_typ)
        length := len(odin_s)
        c_str := strings.clone_to_cstring(odin_s)
        if c_str == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^i64 content")
            return nil, 0, .Text, .Allocation_Error
        }
        return c_str, i32(length), .Text, .None
    case bool:
        s_val_str := "f"
        if val.(bool) { s_val_str = "t" }
        c_str := strings.clone_to_cstring(s_val_str)
        if c_str == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for bool")
            return nil, 0, .Text, .Allocation_Error 
        }
        return c_str, 1, .Text, .None
    case ^bool:
        v_ptr_bool := v_typ
        if v_ptr_bool == nil {
            return nil, 0, .Text, .None // SQL NULL
        }
        v_bool := v_ptr_bool^
        s_val_str := "f"
        if v_bool { s_val_str = "t" }
        c_str := strings.clone_to_cstring(s_val_str)
        if c_str == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for ^bool content")
            return nil, 0, .Text, .Allocation_Error 
        }
        return c_str, 1, .Text, .None
    case time.Time:
        t := a.(time.Time)
        s_val, ok := time.time_to_rfc3339(t)
        if !ok {
            log.errorf("to_pq_param: Failed to convert time.Time to string")
            return nil, 0, .Text, .Allocation_Error
        }
        c_str := strings.clone_to_cstring(s_val)
        length := len(s_val)
        delete(s_val)
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

// connect establishes a new database connection.
// Returns the connection or nil and an error code if connection fails.
connect :: proc(host, port, user, pass, db_name, ssl_mode: string) -> (conn: pq.Conn, err: Err) {
	conn_str_parts := []string{
		"host=", host, " port=", port, " user=", user,
		" password=", pass, " dbname=", db_name, " sslmode=", ssl_mode,
	}
	joined_conn_str := strings.join(conn_str_parts, "")
	defer delete(joined_conn_str)
	conn_str_c := strings.clone_to_cstring(joined_conn_str)
	if conn_str_c == nil {
		log.error("opq.connect: Failed to allocate C string for connection.")
		return nil, .Allocation_Error
	}
	defer delete(conn_str_c)

	conn_obj := pq.connectdb(conn_str_c)
	if conn_obj == nil || pq.status(conn_obj) == .Bad {
		err_msg_pq := ""
		if conn_obj != nil {
			err_msg_odin := to_string(pq.error_message(conn_obj))
			err_msg_pq = err_msg_odin
			log.errorf("opq.connect: Connection failed. PQ Status: %v. Message: %s", pq.status(conn_obj), err_msg_odin)
			delete(err_msg_odin)
			pq.finish(conn_obj)
		} else {
			log.error("opq.connect: Connection failed. pq.connectdb returned nil and no error message retrieval possible.")
		}
		return nil, .Connection_Failed
	}
	return conn_obj, .None
}

// new_migration sets up the necessary database table if it doesn't exist.
create_migration :: proc(conn: pq.Conn, query: cstring) -> (err: Err) {
    result, create_err := exec(conn, query)
    if create_err != .None {
        log.errorf("Failed to execute migration query: %v", create_err)
        return .Migration_Failed
    }
    defer pq.clear(result) // Ensure result is cleared.
    return ok_from_result(conn, result)
}

// delete removes a row from the database.
del :: proc(conn: pq.Conn, query: cstring, arg: any) -> (err: Err) {
	result, exec_err := exec(conn, query, arg)
	if exec_err != .None {
		return exec_err
	}
    if result == nil {
        log.error("opq.del: exec returned nil result without error.")
        return .Query_Failed
    }
	defer pq.clear(result)
	return ok_from_result(conn, result)
}

// exec is an Odin-friendly wrapper for pq.exec_params
// It converts variadic Odin arguments to C params and manages their memory.
// The caller is responsible to clear the returned result.
exec :: proc(conn: pq.Conn, query: cstring, args: ..any) -> (result: pq.Result, err: Err) {
    n_params := len(args)
    if n_params == 0 {
        raw_res := pq.exec(conn, query)
        if raw_res == nil {
            err_msg := to_string(pq.error_message(conn))
            log.error(err_msg)
            delete(err_msg)
            return nil, .Query_Failed
        }
        return raw_res, .None 
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
    result = pq.exec_params(
        conn,
        query,
        i32(n_params),
        nil,                 // param_types (OIDs) - let server infer. Can be specified for more control.
        &param_values_c[0],  // Pointer to the first element
        &param_lengths_c[0],
        &param_formats_c[0],
        .Text,               // result_format (can be configurable, .Binary for performance with some types)
    )
    return result, .None
}

// query_rows executes a query that returns multiple rows and scans them
// into the `dest` slice. `dest` should be a pointer to a slice of structs.
// Manages pq.Result clearing.
query_rows :: proc(conn: pq.Conn, dest_slice_ptr: ^[dynamic]$T, query: cstring, args: ..any) -> Err {
	result, exec_err := exec(conn, query, ..args)
	if exec_err != .None {
		return exec_err
	}
	// exec guarantees a non-nil result if error is .None, but check for safety if that changes.
	if result == nil {
		log.error("opq.query_rows: exec returned nil result unexpectedly (exec_err was .None).")
		return .Query_Failed
	}
	defer pq.clear(result) // CRITICAL: Ensure pq.Result is always cleared.

	num_tuples := pq.n_tuples(result)
	if num_tuples == 0 {
		return .None
	}

	for row_idx: i32 = 0; row_idx < num_tuples; row_idx += 1 {
		current_row_item: T 
		scan_err := scan_row(result, &current_row_item, row_idx)
		if scan_err != .None {
			log.errorf("opq.query_rows: Failed to scan data for row #%d. Error: %v. Query: %s", row_idx, scan_err, query)
			return scan_err
		}
		append(dest_slice_ptr, current_row_item)
	}
	return .None
}

// query_row executes a query expected to return one row (or zero for Not_Found)
// and scan it into the `dest` struct.
query_row :: proc(conn: pq.Conn, dest_struct_ptr: ^$T, query: cstring, args: ..any) -> Err {
	result, exec_err := exec(conn, query, ..args)
	if exec_err != .None {
		return exec_err
	}
	defer pq.clear(result) // CRITICAL: Ensure pq.Result is always cleared.

	num_tuples := pq.n_tuples(result)
	if num_tuples == 0 {
		return .Not_Found
	}
	if num_tuples > 1 {
		log.warnf("opq.query_row: Expected 1 row, got %d. Scanning the first row. Query: %s", num_tuples, query)
		// Depending on strictness, one might return an error here.
	}
	
	return scan_row(result, dest_struct_ptr, 0) // Scan the first row (index 0)
}

scan_row :: proc(result: pq.Result, dest: ^$T, row_idx: i32) -> Err {
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
        col_idx := pq.f_number(result, tag_val_cstr)
        delete(tag_val_cstr)
        if col_idx < 0 {
            return .Column_Not_Found 
        }
        is_null   := pq.get_is_null(result, row_idx, col_idx)
        if is_null {
            return .Is_Nil
        }
        val_ptr   := pq.get_value(result, row_idx, col_idx) 
        val_len   := pq.get_length(result, row_idx, col_idx)
        val := pq_cstr_with_len(val_ptr, val_len)
        scan_err := scan_field(val, dest, s, i)
        if scan_err != .None {
            log.errorf("Failed to scan field %s: %v", s.names[i], scan_err)
            return scan_err
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
    case:
        log.errorf("Unsupported type for field %s: %v", s.names[i], s.types[i])
        // return .Unsupported_Type
    }
    return .None
}

// id_from_result extracts the ID from a result object.
id_from_result :: proc(result: pq.Result) -> (id: i64, err: Err) {
	if result == nil {
		log.error("opq.id_from_result: Received nil result.")
		return -1, .Is_Nil
	}
	// Do not check pq.result_status here if this func is a helper after a successful exec.
	// The caller (exec, query_row, etc.) handles result status and clearing.
	// This function just focuses on extracting data from an assumed valid result.

	if pq.n_tuples(result) != 1 {
		log.errorf("opq.id_from_result: Expected 1 tuple (row) for ID, got %d.", pq.n_tuples(result))
		return -1, .Result_Error
	}
	if pq.n_fields(result) != 1 {
		log.errorf("opq.id_from_result: Expected 1 field (column) for ID, got %d.", pq.n_fields(result))
		return -1, .Result_Error
	}

	if pq.get_is_null(result, 0, 0) {
		log.error("opq.id_from_result: RETURNING id value is NULL.")
		return -1, .Is_Nil
	}

	id_val_ptr := pq.get_value(result, 0, 0)
	id_val_len := pq.get_length(result, 0, 0)

	id_odin_str := pq_cstr_with_len(id_val_ptr, id_val_len)

	new_id, ok := strconv.parse_i64(id_odin_str)
	if !ok {
		log.errorf("opq.id_from_result: Failed to parse RETURNING id from string: '%s'", id_odin_str)
		return -1, .Parsing_Failed
	}
	return new_id, .None
}

// ok_from_result checks if a command result (no tuples expected, or tuples are fine) is okay.
// Does not clear the result; caller is responsible.
ok_from_result :: proc(conn: pq.Conn, result: pq.Result) -> (err: Err) {
	if result == nil {
		// This indicates a problem before even getting a result object,
		// likely a connection issue
		err_msg_odin := to_string(pq.error_message(conn))
		log.errorf("opq.ok_from_result: Received nil result. PQ conn error: %s", err_msg_odin)
		delete(err_msg_odin)
		return .Is_Nil
	}

	status := pq.result_status(result)
	// .Tuples_OK is fine for commands that might return info (e.g. RETURNING)
	// .Command_OK is for commands that don't return rows (INSERT, UPDATE, DELETE without RETURNING)
	// .Single_Tuple for specific single row returns, also acceptable if data extraction is separate.
	if status != .Command_OK && status != .Tuples_OK && status != .Single_Tuple {
		err_msg_odin := to_string(pq.result_error_message(result)) // Error from result
		log.errorf("opq.ok_from_result: Command failed. PQ Status: %v. Message: '%s'", status, err_msg_odin)
		delete(err_msg_odin)
		return .Result_Error
	}
	return .None
}