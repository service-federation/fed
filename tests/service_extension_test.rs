use fed::Parser;
use std::fs;
use tempfile::TempDir;

#[tokio::test]
async fn test_basic_service_extension() {
    // Create a temporary directory structure
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    // Create a package with a base service
    let package_dir = root.join("base-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  web:
    image: nginx:latest
    ports:
      - "80:80"
    environment:
      NGINX_HOST: localhost
      NGINX_PORT: "80"
    expose: true
"#,
    )
    .unwrap();

    // Create a main config that extends the base service
    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./base-package"
    as: "base"

services:
  frontend:
    extends: "base.web"
    environment:
      NGINX_HOST: example.com
      CUSTOM_VAR: custom_value
    ports:
      - "8080:80"
"#,
    )
    .unwrap();

    // Load config with packages
    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    // Verify service was merged correctly
    let frontend = config.services.get("frontend").unwrap();

    // Image should come from base
    assert_eq!(frontend.image.as_deref(), Some("nginx:latest"));

    // Environment should be merged (3 vars: 2 from base, 1 local, 1 override)
    assert_eq!(frontend.environment.len(), 3);
    assert_eq!(
        frontend.environment.get("NGINX_HOST").unwrap(),
        "example.com"
    ); // Local override
    assert_eq!(frontend.environment.get("NGINX_PORT").unwrap(), "80"); // From base
    assert_eq!(
        frontend.environment.get("CUSTOM_VAR").unwrap(),
        "custom_value"
    ); // Local only

    // Ports should be merged (2 unique ports)
    assert_eq!(frontend.ports.len(), 2);
    assert!(frontend.ports.contains(&"80:80".to_string()));
    assert!(frontend.ports.contains(&"8080:80".to_string()));

    // Extends should be cleared
    assert!(frontend.extends.is_none());
}

#[tokio::test]
async fn test_volume_merging_conflict_resolution() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    // Package with base volumes
    let package_dir = root.join("db-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  postgres:
    image: postgres:15
    volumes:
      - "postgres-data:/var/lib/postgresql/data"
      - "./base-config:/etc/postgresql"
    expose: true
"#,
    )
    .unwrap();

    // Main config with conflicting volume
    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./db-package"
    as: "db"

services:
  database:
    extends: "db.postgres"
    volumes:
      - "./local-data:/var/lib/postgresql/data"
      - "./local-logs:/var/log/postgresql"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    let database = config.services.get("database").unwrap();

    // Should have 3 volumes: 2 local + 1 base (non-conflicting)
    assert_eq!(database.volumes.len(), 3);
    assert!(
        database
            .volumes
            .contains(&"./local-data:/var/lib/postgresql/data".to_string())
    ); // Local
    assert!(
        database
            .volumes
            .contains(&"./local-logs:/var/log/postgresql".to_string())
    ); // Local
    assert!(
        database
            .volumes
            .contains(&"./base-config:/etc/postgresql".to_string())
    ); // Base (no conflict)
    assert!(
        !database
            .volumes
            .contains(&"postgres-data:/var/lib/postgresql/data".to_string())
    ); // Base conflicted
}

#[tokio::test]
async fn test_scalar_field_override() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("app-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  app:
    process: npm start
    cwd: /app
    install: npm install
    expose: true
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./app-package"
    as: "app"

services:
  custom-app:
    extends: "app.app"
    process: npm run dev
    cwd: /custom
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    let custom_app = config.services.get("custom-app").unwrap();

    // Local overrides
    assert_eq!(custom_app.process.as_deref(), Some("npm run dev"));
    assert_eq!(custom_app.cwd.as_deref(), Some("/custom"));

    // Base values used where not overridden
    assert_eq!(custom_app.install.as_deref(), Some("npm install"));
}

#[tokio::test]
async fn test_dependency_merging() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("service-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  api:
    image: api:latest
    depends_on:
      - cache
    expose: true
  cache:
    image: redis:latest
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./service-package"
    as: "svc"

services:
  my-api:
    extends: "svc.api"
    depends_on:
      - cache
      - elasticsearch
  cache:
    image: redis:latest
  elasticsearch:
    image: elasticsearch:latest
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    let my_api = config.services.get("my-api").unwrap();

    // Should have 2 unique dependencies (cache appears in both, elasticsearch is new)
    assert_eq!(my_api.depends_on.len(), 2);
    assert!(
        my_api
            .depends_on
            .iter()
            .any(|d| d.service_name() == "cache")
    );
    assert!(
        my_api
            .depends_on
            .iter()
            .any(|d| d.service_name() == "elasticsearch")
    );
}

#[tokio::test]
async fn test_healthcheck_override() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("health-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  service:
    image: test:latest
    healthcheck:
      command: "curl localhost"
    expose: true
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./health-package"
    as: "health"

services:
  my-service:
    extends: "health.service"
    healthcheck:
      http_get: "http://localhost:8080/health"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    let my_service = config.services.get("my-service").unwrap();

    // Local healthcheck should override base
    assert!(my_service.healthcheck.is_some());
    if let Some(hc) = &my_service.healthcheck {
        assert_eq!(hc.get_http_url(), Some("http://localhost:8080/health"));
    }
}

#[tokio::test]
async fn test_multiple_packages_different_services() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    // Package 1: Database
    let db_package_dir = root.join("db-package");
    fs::create_dir(&db_package_dir).unwrap();
    fs::write(
        db_package_dir.join("service-federation.yaml"),
        r#"
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: admin
    expose: true
"#,
    )
    .unwrap();

    // Package 2: Cache
    let cache_package_dir = root.join("cache-package");
    fs::create_dir(&cache_package_dir).unwrap();
    fs::write(
        cache_package_dir.join("service-federation.yaml"),
        r#"
services:
  redis:
    image: redis:7
    ports:
      - "6379:6379"
    expose: true
"#,
    )
    .unwrap();

    // Main config using both packages
    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./db-package"
    as: "db"
  - source: "./cache-package"
    as: "cache"

services:
  database:
    extends: "db.postgres"
    environment:
      POSTGRES_DB: myapp

  redis:
    extends: "cache.redis"
    ports:
      - "6380:6379"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    // Verify both services were extended correctly
    assert_eq!(config.services.len(), 2);

    let database = config.services.get("database").unwrap();
    assert_eq!(database.image.as_deref(), Some("postgres:15"));
    assert_eq!(database.environment.len(), 2);

    let redis = config.services.get("redis").unwrap();
    assert_eq!(redis.image.as_deref(), Some("redis:7"));
    assert_eq!(redis.ports.len(), 2);
}

#[tokio::test]
async fn test_error_package_not_found() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./nonexistent"
    as: "missing"

services:
  test:
    extends: "missing.service"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let result = parser.load_config_with_packages(&main_config_path).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_error_service_not_found_in_package() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("test-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  existing:
    image: test:latest
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./test-package"
    as: "test"

services:
  my-service:
    extends: "test.nonexistent"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let result = parser.load_config_with_packages(&main_config_path).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[tokio::test]
async fn test_error_invalid_extends_format() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("test-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  test:
    image: test:latest
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./test-package"
    as: "test"

services:
  my-service:
    extends: "too.many.parts.here"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let result = parser.load_config_with_packages(&main_config_path).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Invalid extends reference"));
}

#[tokio::test]
async fn test_error_template_not_found() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
services:
  my-service:
    extends: "nonexistent-template"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let result = parser.load_config_with_packages(&main_config_path).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("Template 'nonexistent-template' not found")
    );
}

#[tokio::test]
async fn test_error_circular_extends_in_package() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("circular-package");
    fs::create_dir(&package_dir).unwrap();
    // Package service should not have extends field itself
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  base:
    extends: "other.service"
    image: test:latest
    expose: true
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./circular-package"
    as: "circular"

services:
  my-service:
    extends: "circular.base"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let result = parser.load_config_with_packages(&main_config_path).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Circular"));
}

#[tokio::test]
async fn test_backward_compatibility_no_packages() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
services:
  web:
    image: nginx:latest
    ports:
      - "80:80"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    // Should work without packages
    assert_eq!(config.services.len(), 1);
    let web = config.services.get("web").unwrap();
    assert_eq!(web.image.as_deref(), Some("nginx:latest"));
    assert!(web.extends.is_none());
}

#[tokio::test]
async fn test_parameters_merging() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("param-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  service:
    image: test:latest
    parameters:
      base_param: "base_value"
      override_param: "base_override"
    expose: true
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./param-package"
    as: "param"

services:
  my-service:
    extends: "param.service"
    parameters:
      override_param: "local_override"
      local_param: "local_value"
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    let my_service = config.services.get("my-service").unwrap();

    // Should have 3 parameters: 1 base, 1 local, 1 override
    assert_eq!(my_service.parameters.len(), 3);
    assert_eq!(
        my_service.parameters.get("base_param").unwrap(),
        "base_value"
    );
    assert_eq!(
        my_service.parameters.get("local_param").unwrap(),
        "local_value"
    );
    assert_eq!(
        my_service.parameters.get("override_param").unwrap(),
        "local_override"
    ); // Local wins
}

#[tokio::test]
async fn test_restart_policy_override() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("restart-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  service:
    image: test:latest
    restart: always
    expose: true
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./restart-package"
    as: "restart"

services:
  no-override:
    extends: "restart.service"

  with-override:
    extends: "restart.service"
    restart: no
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    let no_override = config.services.get("no-override").unwrap();
    assert!(matches!(
        no_override.restart,
        Some(fed::RestartPolicy::Always)
    ));

    let with_override = config.services.get("with-override").unwrap();
    assert!(matches!(
        with_override.restart,
        Some(fed::RestartPolicy::No)
    ));
}

#[tokio::test]
async fn test_complex_multi_field_merge() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let package_dir = root.join("complex-package");
    fs::create_dir(&package_dir).unwrap();
    fs::write(
        package_dir.join("service-federation.yaml"),
        r#"
services:
  full-service:
    image: base:latest
    cwd: /app
    install: npm install
    environment:
      BASE_ENV: base_value
      SHARED: base_shared
    volumes:
      - "./base-data:/data"
      - "./base-config:/config"
    ports:
      - "3000:3000"
    depends_on:
      - db
    healthcheck:
      command: "curl localhost:3000"
    restart: always
    expose: true
  db:
    image: postgres:latest
"#,
    )
    .unwrap();

    let main_config_path = root.join("service-federation.yaml");
    fs::write(
        &main_config_path,
        r#"
packages:
  - source: "./complex-package"
    as: "complex"

services:
  customized-service:
    extends: "complex.full-service"
    image: custom:latest
    environment:
      LOCAL_ENV: local_value
      SHARED: local_shared
    volumes:
      - "./local-data:/data"
      - "./local-logs:/logs"
    ports:
      - "3001:3000"
    depends_on:
      - db
      - redis
    healthcheck:
      http_get: "http://localhost:3001/health"
  db:
    image: postgres:latest
  redis:
    image: redis:latest
"#,
    )
    .unwrap();

    let parser = Parser::new();
    let config = parser
        .load_config_with_packages(&main_config_path)
        .await
        .unwrap();

    let service = config.services.get("customized-service").unwrap();

    // Scalar overrides
    assert_eq!(service.image.as_deref(), Some("custom:latest"));

    // Scalar from base (not overridden)
    assert_eq!(service.cwd.as_deref(), Some("/app"));
    assert_eq!(service.install.as_deref(), Some("npm install"));

    // Environment merged
    assert_eq!(service.environment.len(), 3);
    assert_eq!(service.environment.get("BASE_ENV").unwrap(), "base_value");
    assert_eq!(service.environment.get("LOCAL_ENV").unwrap(), "local_value");
    assert_eq!(service.environment.get("SHARED").unwrap(), "local_shared"); // Local wins

    // Volumes merged (conflict resolved)
    assert_eq!(service.volumes.len(), 3); // 2 local + 1 base (non-conflicting)
    assert!(service.volumes.contains(&"./local-data:/data".to_string()));
    assert!(service.volumes.contains(&"./local-logs:/logs".to_string()));
    assert!(
        service
            .volumes
            .contains(&"./base-config:/config".to_string())
    );

    // Ports merged
    assert_eq!(service.ports.len(), 2);

    // Dependencies merged (db appears in both, redis is new)
    assert_eq!(service.depends_on.len(), 2);
    assert!(service.depends_on.iter().any(|d| d.service_name() == "db"));
    assert!(
        service
            .depends_on
            .iter()
            .any(|d| d.service_name() == "redis")
    );

    // Healthcheck overridden
    assert!(service.healthcheck.is_some());
    if let Some(hc) = &service.healthcheck {
        assert_eq!(hc.get_http_url(), Some("http://localhost:3001/health"));
    }

    // Restart from base (not overridden)
    assert!(matches!(service.restart, Some(fed::RestartPolicy::Always)));

    // Extends cleared
    assert!(service.extends.is_none());
}
