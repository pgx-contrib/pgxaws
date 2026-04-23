# Changelog

## 1.0.0 (2026-04-23)


### Features

* **connector:** add Authorizer interface and support for DSQL authorization ([85dd4f1](https://github.com/pgx-contrib/pgxaws/commit/85dd4f1751ef028c7d53ae2abbccd7f83a285835))
* **connector:** add AWS DSQL IAM authentication support ([aec7504](https://github.com/pgx-contrib/pgxaws/commit/aec75045bc0dfee15ec7bd67a0098db9287934cf))
* **connector:** extract RDS IAM auth logic to RDSAuth struct ([e29e369](https://github.com/pgx-contrib/pgxaws/commit/e29e369d3e88a996bc0c852c1bd9bb7db13689da))
* improve code clarity and error handling in cacher and connector ([5ae1022](https://github.com/pgx-contrib/pgxaws/commit/5ae1022073fce64025c769cd33c06da7b27ab865))


### Bug Fixes

* address race conditions, S3 TTL semantics, and resource leaks ([0259575](https://github.com/pgx-contrib/pgxaws/commit/0259575f0a8d5ab542b665b2cbea5e2585bee9c5))
* update CI badge link from ci.yaml to ci.yml ([c39fc1c](https://github.com/pgx-contrib/pgxaws/commit/c39fc1c3d427c8fbb0724ab7f0c19ede9276b6eb))
