use sqlite::{context, value};
use sqlite_nostd as sqlite;

// Global context for logging - will be set during initialization
static mut DEBUG_ENABLED: bool = false;

pub fn debug_log(msg: &str) {
    unsafe {
        if DEBUG_ENABLED {
            libc_print::libc_println!("[DEBUG] {}", msg);
        }
    }
}

pub unsafe extern "C" fn x_crsql_set_debug(ctx: *mut context, argc: i32, argv: *mut *mut value) {
    if argc == 0 {
        // If no arguments, return current state
        sqlite::result_int(ctx, if DEBUG_ENABLED { 1 } else { 0 });
        return;
    }

    if argc > 1 {
        // Too many arguments
        return;
    }

    let enabled = {
        let arg = *argv;
        sqlite::value_int(arg) != 0
    };

    DEBUG_ENABLED = enabled;

    // Return success (the new state)
    sqlite::result_int(ctx, if enabled { 1 } else { 0 });
}
