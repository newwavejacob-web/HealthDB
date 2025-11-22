// actually storage operations??

pub struct Database {

}

impl Database {
    pub fn new() -> Self {
        todo!()
    }
    pub fn set(&mut self, key: String, value: String) {
        todo!()
    }
    //option return type is how we get some and none if get works or not
    pub fn get(&self, key: &str) -> Option<String> {
        todo!()
    }
    pub fn delete(&mut self, key: &str) -> bool {
        todo!()
    }
}
