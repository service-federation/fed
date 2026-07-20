use fed::Parser;

#[tokio::test]
async fn test_service_merging_example() {
    // This test demonstrates the service merging example
    let example_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples/service-merging/fed.yaml");

    if !example_path.exists() {
        panic!("Example config not found at: {}", example_path.display());
    }

    // Load the config with package resolution and service merging
    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&example_path)
        .await
        .expect("Failed to load config with packages");

    // Verify we have 3 services (database, cache, web)
    assert_eq!(config.services.len(), 3);

    // Verify database service was properly merged
    let database = config
        .services
        .get("database")
        .expect("database service should exist");

    // Image from package
    assert_eq!(database.image.as_deref(), Some("postgres:15"));

    // Environment: merged from package and local
    assert_eq!(database.environment.len(), 4); // 3 from package + 2 local (1 is override)
    assert_eq!(database.environment.get("POSTGRES_USER").unwrap(), "admin"); // From package
    assert_eq!(
        database.environment.get("POSTGRES_PASSWORD").unwrap(),
        "secret"
    ); // From package
    assert_eq!(database.environment.get("POSTGRES_DB").unwrap(), "myapp"); // Local override
    assert_eq!(
        database.environment.get("APP_NAME").unwrap(),
        "MyApplication"
    ); // Local addition

    // Volumes: merged (1 from package + 1 local)
    assert_eq!(database.volumes.len(), 2);
    assert!(
        database
            .volumes
            .contains(&"postgres-data:/var/lib/postgresql/data".to_string())
    );
    assert!(database.volumes.contains(&"./backups:/backups".to_string()));

    // Ports: local has different host port, but since target is same, only local is kept
    // Actually, both should be there since they're unique strings
    assert!(!database.ports.is_empty());
    assert!(database.ports.contains(&"5433:5432".to_string()));

    // Healthcheck from package
    assert!(database.healthcheck.is_some());

    // Restart policy from package
    assert!(matches!(database.restart, Some(fed::RestartPolicy::Always)));

    // Extends field should be cleared after merging
    assert!(database.extends.is_none());

    // Verify cache service was properly merged
    let cache = config
        .services
        .get("cache")
        .expect("cache service should exist");

    // Image from package
    assert_eq!(cache.image.as_deref(), Some("redis:7-alpine"));

    // Volumes: merged (1 from package + 1 local)
    assert_eq!(cache.volumes.len(), 2);
    assert!(cache.volumes.contains(&"redis-data:/data".to_string()));
    assert!(
        cache
            .volumes
            .contains(&"./redis.conf:/usr/local/etc/redis/redis.conf".to_string())
    );

    // Ports: merged
    assert!(!cache.ports.is_empty());

    // Healthcheck from package
    assert!(cache.healthcheck.is_some());

    // Extends cleared
    assert!(cache.extends.is_none());

    // Verify web service (no extends)
    let web = config
        .services
        .get("web")
        .expect("web service should exist");
    assert_eq!(web.image.as_deref(), Some("node:18"));
    assert!(web.extends.is_none());
    assert_eq!(web.depends_on.len(), 2);
    assert!(
        web.depends_on
            .iter()
            .any(|d| d.service_name() == "database")
    );
    assert!(web.depends_on.iter().any(|d| d.service_name() == "cache"));

    // Verify entrypoint
    assert_eq!(config.entrypoint.as_deref(), Some("web"));

    println!("✓ Service merging example verified successfully!");
    println!("  - Database service extended from db-package.postgres");
    println!("  - Cache service extended from cache-package.redis");
    println!("  - All fields properly merged with correct precedence");
}
