use alloc::{boxed::Box, collections::BTreeMap, vec::Vec};
use core::{
    ffi::{c_int, c_void},
    mem,
    ptr::null,
};

use sqlite_nostd::ResultCode;

use crate::c::crsql_ExtData;
use crate::tableinfo::TableInfo;

#[no_mangle]
pub unsafe extern "C" fn crsql_commit_hook(user_data: *mut c_void) -> c_int {
    let ext_data = user_data as *mut crsql_ExtData;

    if (*ext_data).pendingDbVersion > -1 {
        (*ext_data).dbVersion = (*ext_data).pendingDbVersion;
    }

    commit_or_rollback_reset(ext_data);

    ResultCode::OK as c_int
}

#[no_mangle]
pub unsafe extern "C" fn crsql_rollback_hook(user_data: *mut c_void) -> *const c_void {
    commit_or_rollback_reset(user_data as *mut crsql_ExtData);
    null()
}

pub unsafe fn commit_or_rollback_reset(ext_data: *mut crsql_ExtData) {
    (*ext_data).pendingDbVersion = -1;
    (*ext_data).seq = 0;
    (*ext_data).timestamp = 0;
    (*ext_data).updatedTableInfosThisTx = 0;

    let mut ordinals: mem::ManuallyDrop<Box<BTreeMap<Vec<u8>, i64>>> = mem::ManuallyDrop::new(
        Box::from_raw((*ext_data).ordinalMap as *mut BTreeMap<Vec<u8>, i64>),
    );

    let mut table_infos = unsafe {
        mem::ManuallyDrop::new(Box::from_raw((*ext_data).tableInfos as *mut Vec<TableInfo>))
    };

    let mut last_db_versions: mem::ManuallyDrop<Box<BTreeMap<Vec<u8>, i64>>> =
        mem::ManuallyDrop::new(Box::from_raw(
            (*ext_data).lastDbVersions as *mut BTreeMap<Vec<u8>, i64>,
        ));

    last_db_versions.clear();
    ordinals.clear();
    for tbl_info in table_infos.iter_mut() {
        tbl_info.clear_cl_cache();
    }
}
