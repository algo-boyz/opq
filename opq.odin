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
    Bad_Parameter = 3,
    Query_Failed = 4,
    Parsing_Failed = 5,
    Not_Found = 6,
    Is_Nil = 7, 
    Allocation_Error = 8,
    Time_Format_Error = 9,
    Column_Not_Found = 10,
}

BOOLOID : u32 = 16
BYTEAOID : u32 = 17
CHAROID : u32 = 18
NAMEOID : u32 = 19
INT8OID : u32 = 20  // BIGINT / BIGSERIAL
INT2OID : u32 = 21
INT4OID : u32 = 23
TEXTOID : u32 = 25
OIDOID : u32 = 26
JSONOID : u32 = 114
FLOAT4OID : u32 = 700
FLOAT8OID : u32 = 701
UNKNOWNOID : u32 = 705
VARCHAROID : u32 = 1043
DATEOID : u32 = 1082
TIMEOID : u32 = 1083
TIMESTAMPOID : u32 = 1114
TIMESTAMPTZOID : u32 = 1184 // For TIMESTAMP WITH TIME ZONE
NUMERICOID : u32 = 1700
UUIDOID : u32 = 2950
JSONBOID : u32 = 3802

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

pq_cstr_with_len :: proc(data_ptr: [^]byte, length: i32) -> string {
    if data_ptr == nil || length <= 0 {
        return ""
    }
    bytes_slice := ([^]u8)(data_ptr)
    return string(bytes_slice[:length])
}

// to_pq_param_single converts a value to pq parameter parts.
to_pq_param :: proc(val: any) -> (p_val: cstring, p_len: i32, p_fmt: pq.Format, err_code: Err) {
    if val == nil { // Handles untyped nil passed directly for SQL NULL
        return nil, 0, .Text, .None
    }
    raw_type_info := type_info_of(val.id)
	ti := runtime.type_info_base(raw_type_info)
	a := any{val.data, ti.id}
	switch v_typed in a {
    case string:
        v_str := a.(string)
        p_val = strings.clone_to_cstring(v_str)
        if p_val == nil {
            log.errorf("to_pq_param: strings.clone_to_cstring failed for string")
            return nil, 0, .Text, .Allocation_Error
        }
        return p_val, i32(len(v_str)), .Text, .None
    case ^string:
        v_ptr_str := v_typed
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
    case i64:
        odin_s := fmt.tprintf("%d", v_typed)
        p_val = strings.clone_to_cstring(odin_s)
        length := len(odin_s)
        if p_val == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for i64")
            return nil, 0, .Text, .Allocation_Error 
        }
        return p_val, i32(length), .Text, .None
    case bool:
        s_val_str := "false"
        if val.(bool) { s_val_str = "true" }
        temp_odin_s := s_val_str
        if !val.(bool) { temp_odin_s = "false"} else {temp_odin_s = "true"}
        c_str := strings.clone_to_cstring(temp_odin_s)
        actual_odin_str: string
        if val.(bool) { actual_odin_str = "true" } else { actual_odin_str = "false" }
        c_str = strings.clone_to_cstring(actual_odin_str)
        
        if c_str == nil { 
            log.errorf("to_pq_param: strings.clone_to_cstring failed for bool")
            return nil, 0, .Text, .Allocation_Error 
        }
        return c_str, i32(len(actual_odin_str)), .Text, .None
    // case time.Time: // TODO
        // t := v_typed.(time.Time)
        // s_val := time.format_rfc3339(t)
        // c_str := strings.clone_to_cstring(s_val)
        // length := len(s_val)
        // delete(s_val) // delete s_val after cloning
        // if c_str == nil { return nil, 0, .Text, .Allocation_Error }
        // return c_str, i32(length), .Text, .None
    case:
        log.errorf("to_pq_param: Unsupported type: %v for value %v", v_typed, val)
    }
    return nil, 0, .Text, .Bad_Parameter
}

connect :: proc(host, port, user, pass, db_name, ssl_mode: string) -> (conn: pq.Conn) {
    conn_str_parts := []string{
        "host=", host, " port=", port, " user=", user,
        " password=", pass, " dbname=", db_name, " sslmode=", ssl_mode,
    }
    join_str := strings.join(conn_str_parts, "")
    defer delete(join_str) 
    conn_str_c := strings.clone_to_cstring(join_str)
    defer delete(conn_str_c)

    conn = pq.connectdb(conn_str_c)
    if conn == nil || pq.status(conn) == .Bad {
        err_msg := ""
        if conn != nil { err_msg = to_string(pq.error_message(conn)); pq.finish(conn) }
        log.error(err_msg)
        delete(err_msg)
        os.exit(-1)
    }
    return conn
}

// new_migration sets up the necessary database table if it doesn't exist.
new_migration :: proc(conn: pq.Conn, query: cstring) -> (err: Err) {
    result, create_err := exec(conn, query)
    if create_err != .None {
        log.errorf("Failed to execute migration query: %v", create_err)
        return .Migration_Failed
    }
    if result == nil {
        err_msg := to_string(pq.error_message(conn))
        log.error(err_msg)
        delete(err_msg)
        return .Migration_Failed
    }
    defer pq.clear(result)

    status := pq.result_status(result)
    if status != .Command_OK && status != .Tuples_OK {
        log.error(pq.result_error_message(result))
        return .Migration_Failed
    }
    log.info("migration successful")
    return .None
}

// exec is an Odin-friendly wrapper for exec_params.
// It converts variadic Odin arguments to C parameters and manages their memory.
// The caller is responsible for calling clear() on the returned result.
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
    defer {
        for val in param_values_c {
            free(val)
        }
    }
    if result == nil {
        // This case might occur if exec_params itself fails before creating a result object,
        // e.g. due to bad parameters passed to the C function or out of mem within lib
        err_msg := to_string(pq.error_message(conn))
        log.error(err_msg)
        delete(err_msg)
        return nil, .Query_Failed
    }
    return result, .None
}

// query_row executes a query expected to return one row (or zero for Not_Found)
// and scan it into the `dest` struct.
// Manages Result clearing.
query_row :: proc(conn: pq.Conn, dest: ^$T, query: cstring, args: ..any) -> Err {
    result, exec_err := exec(conn, query, ..args)
    if exec_err != .None {
        return exec_err
    }
    if result == nil {
        log.error("exec returned nil result.")
        return .Query_Failed 
    }
    defer pq.clear(result) // Ensure result is cleared.

    status := pq.result_status(result)
    if status != .Tuples_OK {
        log.error(pq.result_error_message(result))
        return .Query_Failed 
    }
    num_tuples := pq.n_tuples(result)
    if num_tuples == 0 {
        return .Not_Found
    }
    if num_tuples > 1 {
        log.warnf("Expected 1 row, got %d. Scanning the first row", num_tuples)
        // Depending on strictness, could return an error: .Multiple_Rows_Found_For_QueryRow
    }
    row_idx:i32 // Scan the first row
    ti := runtime.type_info_base(type_info_of(T))
	s, ok := ti.variant.(runtime.Type_Info_Struct)
    if !ok {
        return .Bad_Parameter
    }
    for i:i32; i < s.field_count; i += 1 {
        tag_val, has_tag := get_tag_val(s.tags[i])
        if !has_tag {
            fmt.println("No db tag found for field", i, "skipping")
            // If no db tag, this field is not mapped from the database. Skip it.
            continue
        }
        // Find the column index in the result set for this db_tag_val.
        tag_val_cstr := strings.clone_to_cstring(tag_val)
        if tag_val_cstr == nil {
            log.errorf("Failed to convert db_tag '%s' to cstring for field '%s'", tag_val, tag_val)
            return .Allocation_Error 
        }
        col_idx := pq.f_number(result, tag_val_cstr)
        delete(tag_val_cstr) // Clean up the temporary cstring
        if col_idx < 0 {
            // Column specified in tag not found in result set.
            return .Column_Not_Found 
        }
        is_null   := pq.get_is_null(result, row_idx, col_idx)
        if is_null {
            return .Is_Nil
        }
        val_ptr   := pq.get_value(result, row_idx, col_idx) 
        val_len   := pq.get_length(result, row_idx, col_idx)
        val := pq_cstr_with_len(val_ptr, val_len)
        fmt.printfln("Field %s: %s", s.names[i], val)
        fields := reflect.struct_field_types(T)
        f := reflect.struct_field_value_by_name(dest, s.names[i])
        switch fields[i].id {
        case i64:
            new_i64, ok := strconv.parse_i64(val)
            if !ok {
                log.errorf("Failed to parse i64 from string: '%s'", val)
                return .Parsing_Failed
            }
            (^i64)(f.data)^ = new_i64
        case string:
            (^string)(f.data)^ = val
        case:
            log.errorf("Unsupported type for field %s: %v", s.names[i], s.types[i])
        }
    }
    return .None
}

id_from_result :: proc(result: pq.Result) -> (id: i64, err: Err) {
    if result == nil {
        return -1, .Query_Failed
    }
    defer pq.clear(result)

    status := pq.result_status(result)
    if status != .Tuples_OK {
        log.error(pq.result_error_message(result))
        return -1, .Query_Failed
    }
    if pq.n_tuples(result) != 1 || pq.n_fields(result) != 1 {
        log.error("Unexpected number of rows/fields from RETURNING id")
        return -1, .Query_Failed // Or more specific error
    }
    id_val_ptr := pq.get_value(result, 0, 0)
    id_val_len := pq.get_length(result, 0, 0)
    if pq.get_is_null(result, 0, 0) || id_val_ptr == nil {
        log.error("RETURNING id gave NULL value.")
        return -1, .Parsing_Failed
    }
    id_str_view := pq_cstr_with_len(id_val_ptr, id_val_len)
    
    new_id, ok := strconv.parse_i64(id_str_view)
    if !ok {
        log.errorf("Failed to parse RETURNING id from string: '%s'", id_str_view)
        return -1, .Parsing_Failed
    }
    return new_id, .None
}
