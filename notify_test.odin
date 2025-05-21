package opq

import "core:fmt"
import "core:log"
import "core:strings"
import "core:testing"
import "core:time"
import "pq"

@(test)
test_single_notification :: proc(t: ^testing.T) {
    cfg := default_config()
    conn_str_c := strings.clone_to_cstring(fmt.tprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
        cfg.host, cfg.port, cfg.user, cfg.pass, cfg.db_name, cfg.ssl_mode))
    defer delete(conn_str_c)

    conn := pq.connectdb(conn_str_c)
    if conn == nil || pq.status(conn) == .Bad {
        cerr:= pq.error_message(conn)
        err_msg := ""
        if cerr != nil { err_msg = to_string(cerr) }
        log.errorf("Failed to connect to database: %s", err_msg)
        if err_msg != "" { delete(err_msg) }
        if conn != nil { pq.finish(conn) }
        testing.fail(t)
        return
    }
    defer pq.finish(conn)
    log.info("Successfully connected to database for notifier test.")

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
        unlisten(conn, test_channel)
        testing.fail(t)
        return
    }
    log.infof("Sent NOTIFY to '%s' with payload '%s'", test_channel, test_payload)
    // Small delay
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
    // 5. Cleanup
    if notifications != nil {
        for i := 0; i < len(notifications); i += 1 {
            delete_notification(&notifications[i])
        }
        delete(notifications)
    }
    unlisten_err := unlisten(conn, test_channel)
    if unlisten_err != Err.None {
        log.warnf("unlisten from channel '%s' failed: %v", test_channel, unlisten_err)
    } else {
        log.infof("Successfully unlistened from channel '%s'", test_channel)
    }
}