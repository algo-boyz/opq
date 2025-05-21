package opq

import "pq"
import "core:log"
import "core:strings"
import "core:fmt"

// Construct "LISTEN <channel_name>" query
listen :: proc(conn: pq.Conn, channel: string) -> Err {
    query_str := fmt.tprintf("LISTEN %s", channel)
    query_c := strings.clone_to_cstring(query_str)
    if query_c == nil {
        return .Allocation_Error
    }
    defer delete(query_c)

    res, err_code := exec(conn, query_c)
    if err_code != .None {
        return err_code
    }
    defer pq.clear(res)
    return ok_from_result(conn, res)
}

// Construct "UNLISTEN <channel_name>" or "UNLISTEN *"
unlisten :: proc(conn: pq.Conn, channel: string) -> Err {
    query_str := fmt.tprintf("UNLISTEN %s", channel)
    query_c := strings.clone_to_cstring(query_str)
    if query_c == nil {
        return .Allocation_Error
    }
    defer delete(query_c)

    res, err_code := exec(conn, query_c)
    if err_code != .None {
        return err_code
    }
    defer pq.clear(res)
    return ok_from_result(conn, res)
}

// Register_Event_Handler allows users to register their libpq event callbacks.
// The 'name' is used by libpq for error messages.
// 'pass_through' is user-defined data passed to every invocation of the callback for this registration.
register_event_handler :: proc(conn: pq.Conn, proc_func: pq.Event_Proc, name: string, pass_through_data: rawptr) -> (success: bool, err: Err) {
    if conn == nil || proc_func == nil {
        return false, .Bad_Parameter
    }
    name_c := strings.clone_to_cstring(name)
    if name_c == nil {
        return false, .Allocation_Error
    }
    defer delete(name_c)

    if !pq.register_event_proc(conn, proc_func, name_c, pass_through_data) {
        // Registration failed, libpq might have more info in its error message system,
        // though register_event_proc itself doesn't set pq.error_message typically.
        // It's often due to bad params or out of memory internally.
        log.errorf("opq.register_event_handler: pq.register_event_proc failed for '%s'", name)
        return false, .Result_Error // Or a more specific "EventHandlerRegistrationFailed"
    }
    return true, .None
}

/* User's event callback example
my_event_callback :: proc "c" (evt_id: pq.Event_ID, evt_info: rawptr, pass_through: rawptr) -> b32 {
    conn_from_event: pq.Conn
    res_from_event: pq.Result

    #partial switch evt_id {
    case .Conn_Destroy:
        evt_data := cast(^pq.Event_Conn_Destroy) evt_info
        conn_from_event = evt_data.conn
        // Retrieve and free associated instance data
        my_conn_data_ptr := cast(^My_Conn_Specific_Data) pq.instance_data(conn_from_event, my_event_callback)
        if my_conn_data_ptr != nil {
            // ... free resources within my_conn_data_ptr ...
            // free(my_conn_data_ptr)
            pq.set_instance_data(conn_from_event, my_event_callback, nil) // Clear it
        }
    case .Result_Create:
        evt_data := cast(^pq.Event_Result_Create) evt_info
        res_from_event = evt_data.result
        // Associate data with this specific result for this event proc
        // my_result_data_ptr := new(My_Result_Specific_Data)
        // pq.result_set_instance_data(res_from_event, my_event_callback, my_result_data_ptr)
    case .Result_Destroy:
        evt_data := cast(^pq.Event_Result_Destroy) evt_info
        res_from_event = evt_data.result
        // Retrieve and free associated result instance data
        // my_result_data_ptr := pq.result_instance_data(res_from_event, my_event_callback)
        // free(my_result_data_ptr)
        // pq.result_set_instance_data(res_from_event, my_event_callback, nil)
    }
    return true
}
My_Conn_Specific_Data :: struct { count: int, name: string }
My_Result_Specific_Data :: struct { processed_rows: int }
*/

Notification :: struct {
    channel: string,
    payload: string,
    be_pid:  i32,
}

// delete_notification frees all strings within the Notification struct
delete_notification :: proc(n: ^Notification) {
    if n == nil { return }
    delete(n.channel)
    delete(n.payload)
}

// notify_channel sends a notification to the specified channel with an optional payload.
notify_channel :: proc(conn: pq.Conn, channel: string, payload: string) -> Err {
    query: string
    if payload != "" {
        escaped_payload, ok := strings.replace_all(payload, "'", "''") // Basic escaping for SQL strings
        if !ok {
            log.error("notify_channel: Failed to escape payload.")
            return Err.Allocation_Error
        }
        defer delete(escaped_payload)
        query = fmt.tprintf("NOTIFY %s, '%s'", channel, escaped_payload)

    } else {
        query = fmt.tprintf("NOTIFY %s", channel)
    }
    query_c := strings.clone_to_cstring(query)
    if query_c == nil {
        log.error("notify_channel: Failed to allocate C string for query.")
        return Err.Allocation_Error
    }
    defer delete(query_c)
    res, err_code := exec(conn, query_c) 
    if err_code != Err.None {
        log.errorf("notify_channel: exec failed: %v", err_code)
        return err_code
    }
    defer pq.clear(res)
    return ok_from_result(conn, res)
}

// consume_notifications checks for and processes any pending notifications.
// It returns a dynamic array of opq.Notification and an error code.
// The caller is responsible for deleting the returned slice and its elements.
consume_notifications :: proc(conn: pq.Conn) -> (notifications: [dynamic]Notification, err: Err) {
    if !pq.consume_input(conn) {
        err_msg := to_string(pq.error_message(conn))
        log.error(err_msg)
        delete(err_msg)
        return nil, .Connection_Failed
    }

    notifs_list: [dynamic]Notification
    for {
        pq_notify_ptr := pq.notifies(conn)
        if pq_notify_ptr == nil {
            break // No more notifications
        }
        // Convert pq.Notify to opq.Notification
        channel_str := to_string(pq_notify_ptr.relname)
        payload_str := to_string(pq_notify_ptr.extra)
        append(&notifs_list, Notification{
            channel = channel_str,
            payload = payload_str,
            be_pid  = pq_notify_ptr.be_pid,
        })
        // IMPORTANT: pq.notifies() returns a pointer to an internally managed linked list.
        // The memory for the pq.Notify struct itself is managed by libpq and freed
        // when the parent pq.Conn is closed or on the next call to pq.notifies() that
        // rebuilds the list. We've cloned the strings, so those are the caller's responsibility.
    }
    if len(notifs_list) > 0 {
        return notifs_list, .None
    }
    return nil, .None
}

/* Example usage:
main :: proc() {
    opq.listen(conn, "my_chan")
    for !should_quit {
        notifications, err := opq.consume_notifications(conn)
        if err == .None && notifications != nil {
            for notif in notifications {
                fmt.printf("Received notification on channel '%s': %s (PID: %d)\n", notif.channel, notif.payload, notif.be_pid)
                opq.delete_notification(&notif)
            }
            delete(notifications)
        } else if err != .None {
            log.errorf("Error consuming notifications: %v", err)
         }
     }
}
*/