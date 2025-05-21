package opq

import "core:fmt"
import "core:log"
import "core:strings"
import "core:testing"
import "core:time"
import "pq"

// Helper to send a NOTIFY command.
// This would typically use exec if it's part of your opq package,
// or you can define it as a test utility.
notify_channel :: proc(conn: pq.Conn, channel: string, payload: string) -> Err {
    // Ensure payload is properly escaped for SQL if it can contain special characters.
    // For simple payloads like in this test, direct inclusion is often fine.
    // For production, consider using placeholders if your exec supports it, or ensure robust escaping.
    query: string
    if payload != "" {
        // libpq's NOTIFY syntax for payload: NOTIFY channel, 'payload'
        // The payload is an arbitrary string. It needs to be a valid SQL string literal.
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

    // These exec and ok_from_result are assumed to exist in your opq package
    res, err_code := exec(conn, query_c) 
    if err_code != Err.None {
        log.errorf("notify_channel: exec failed: %v", err_code)
        return err_code
    }
    defer pq.clear(res) // Ensure result is cleared

    return ok_from_result(conn, res)
}

@(test)
test_single_notification :: proc(t: ^testing.T) {
    // Database connection parameters
    // TODO: Use environment variables or a config file for these in real test suites
    cfg := PoolConfig{ // Using PoolConfig structure to hold connection params
        host     = "localhost",
        port     = "5432",
        user     = "postgres",  // Replace with your test user
        pass     = "password", // Replace with your test password
        db_name  = "war",      // Replace with your test database (ensure it exists)
        ssl_mode = "disable",
    }

    // Establish a connection using an assumed connect
    // This connect is different from the pool's connect; it's a direct one.
    // If connect isn't part of your public API, use the raw pq.connectdb here.
    conn_str_c := strings.clone_to_cstring(fmt.tprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
        cfg.host, cfg.port, cfg.user, cfg.pass, cfg.db_name, cfg.ssl_mode))
    defer delete(conn_str_c)

    conn := pq.connectdb(conn_str_c)
    if conn == nil || pq.status(conn) == .Bad {
        err_msg_c := pq.error_message(conn)
        err_msg_odin := ""
        if err_msg_c != nil { err_msg_odin = to_string(err_msg_c) } // Assumes to_string
        log.errorf("Failed to connect to database: %s", err_msg_odin)
        if err_msg_odin != "" { delete(err_msg_odin) }
        if conn != nil { pq.finish(conn) }
        testing.fail(t)
        return
    }
    defer pq.finish(conn)
    log.info("Successfully connected to database for notifier test.")

    // Generate a unique channel name for this test run to avoid collisions
    test_channel := fmt.tprintf("test_notify_chan_%d", time.now()._nsec)
    test_payload := "Odin 'Test Notification! 🎉'"

    // 1. Listen on the channel
    listen_err := listen(conn, test_channel)
    if listen_err != Err.None {
        log.errorf("listen on channel '%s' failed: %v", test_channel, listen_err)
        testing.fail(t)
        return
    }
    log.infof("Listening on channel '%s'", test_channel)

    // 2. Send a notification
    notify_err := notify_channel(conn, test_channel, test_payload)
    if notify_err != Err.None {
        log.errorf("notify_channel to '%s' failed: %v", test_channel, notify_err)
        unlisten(conn, test_channel) // Attempt cleanup
        testing.fail(t)
        return
    }
    log.infof("Sent NOTIFY to '%s' with payload '%s'", test_channel, test_payload)
    
    // Small delay: In some CI environments or very fast machines, 
    // a tiny delay can help ensure the notification is processed by the backend 
    // and available for consumption. pq.consume_input should make this robust,
    // but it's a common pattern in notification tests.
    time.sleep(50 * time.Millisecond) 

    // 3. Consume notifications
    notifications, consume_err := consume_notifications(conn)
    if consume_err != Err.None {
        log.errorf("consume_notifications failed: %v", consume_err)
        unlisten(conn, test_channel) // Attempt cleanup
        testing.fail(t)
        return
    }

    // 4. Validate the notification
    if notifications == nil || len(notifications) == 0 {
        log.errorf("No notifications received on channel '%s'. Expected 1.", test_channel)
        testing.fail(t)
    } else if len(notifications) != 1 {
        log.errorf("Expected 1 notification on channel '%s', got %d", test_channel, len(notifications))
        testing.fail(t)
    } else {
        received_notif := notifications[0]
        log.infof("Received notification: channel='%s', payload='%s', pid=%d",
            received_notif.channel, received_notif.payload, received_notif.be_pid)

        if received_notif.channel != test_channel {
            log.errorf("Notification channel mismatch: expected '%s', got '%s'", test_channel, received_notif.channel)
            testing.fail(t)
        }
        if received_notif.payload != test_payload {
            log.errorf("Notification payload mismatch: expected '%s', got '%s'", test_payload, received_notif.payload)
            testing.fail(t)
        }
    }

    // 5. Cleanup notifications memory
    if notifications != nil {
        for i := 0; i < len(notifications); i += 1 {
            // The delete_notification proc should handle nil strings within the notification
            delete_notification(&notifications[i])
        }
        delete(notifications) // delete the slice itself
    }

    // 6. Unlisten from the channel
    unlisten_err := unlisten(conn, test_channel)
    if unlisten_err != Err.None {
        // Log error but don't necessarily fail the test if primary functionality worked
        log.warnf("unlisten from channel '%s' failed: %v", test_channel, unlisten_err)
    } else {
        log.infof("Successfully unlistened from channel '%s'", test_channel)
    }

    // testing.T will automatically track if testing.fail(t) was called.
}

// To run this test:
// 1. Ensure your opq package and its dependencies (like pq) are accessible.
// 2. Compile and run: `odin test path/to/tests -vet -strict-style`
//    (or wherever your notifier_test.odin file is located)