use core::sync::atomic::AtomicU64;

// Named `Ydb` (not `ydb`) to mirror the proto package `Ydb` and its generated paths.
#[expect(non_snake_case)]
pub mod Ydb {
    #![allow(clippy::pedantic, clippy::nursery, clippy::restriction)]
    // Rust module names MUST be lowercase to match proto package names:
    // package Ydb → include!("ydb.rs")
    // package Ydb.Issue → include!("ydb.issue.rs") → mod issue (lowercase!)
    // package Ydb.PersQueue.V1 → include!("ydb.pers_queue.v1.rs")

    include!(concat!(env!("OUT_DIR"), "/ydb.rs"));

    pub mod pers_queue {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/ydb.pers_queue.v1.rs"));
        }
        pub mod cluster_discovery {
            include!(concat!(env!("OUT_DIR"), "/ydb.pers_queue.cluster_discovery.rs"));
        }
    }
    pub mod discovery {
        include!(concat!(env!("OUT_DIR"), "/ydb.discovery.rs"));
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/ydb.discovery.v1.rs"));
        }
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
}

pub mod config;
pub mod middleware;
pub mod parser;
pub mod partition;
pub mod pipeline;
pub mod providers;
pub mod serializer;
pub mod types;

static BATCH_ID: AtomicU64 = AtomicU64::new(1);

// `pub(in crate)` is required: `pub_with_shorthand`/`pub_without_shorthand`
// are conflicting restriction lints, so either spelling needs an expect.
#[expect(clippy::pub_without_shorthand, reason = "explicit `in` visibility per pub_with_shorthand")]
pub(in crate) fn batch_id() -> u64 {
    BATCH_ID.fetch_add(1, core::sync::atomic::Ordering::Relaxed)
}
