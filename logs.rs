use crate::store;

type LogFile = //however you spawn in a log file

// call these in store.rs, when called it will append to the log file
pub fn log_set() {

}
pub fn log_get() {

}
pub fn log_del() {

}

// this is the tricky part. how do i make it so that if my db detects the log file != empty, then
// it reloads all of the operations on the hash map
pub fn reload() {

}
