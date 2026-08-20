use std::collections::BTreeSet;

use crate::routes::API_ROUTES;

#[test]
fn route_manifest_has_unique_names_and_method_paths() {
    let mut names = BTreeSet::new();
    let mut transports = BTreeSet::new();
    for route in API_ROUTES {
        assert!(
            names.insert(route.name),
            "duplicate route name {}",
            route.name
        );
        assert!(
            transports.insert((route.method, route.path)),
            "duplicate route transport {} {}",
            route.method,
            route.path
        );
        assert!(route.response.ends_with("_response"));
    }
}
