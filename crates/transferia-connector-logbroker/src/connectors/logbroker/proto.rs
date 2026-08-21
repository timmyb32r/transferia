#![allow(clippy::pedantic, clippy::nursery, clippy::restriction)]

include!(concat!(env!("OUT_DIR"), "/ydb.rs"));

pub mod pers_queue {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/ydb.pers_queue.v1.rs"));
    }
}

pub mod discovery {
    include!(concat!(env!("OUT_DIR"), "/ydb.discovery.rs"));
}

pub mod operations {
    include!(concat!(env!("OUT_DIR"), "/ydb.operations.rs"));
}

pub mod issue {
    include!(concat!(env!("OUT_DIR"), "/ydb.issue.rs"));
}

pub mod scheme {
    include!(concat!(env!("OUT_DIR"), "/ydb.scheme.rs"));
}
