# Changelog

## 1.0.0 (2026-11-11)


### Features

* **ci:** read the ralch Nix cache ([#485](https://github.com/pgx-contrib/pgxaws/issues/485)) ([3e2136b](https://github.com/pgx-contrib/pgxaws/commit/3e2136bc3845d73f2b9cff6f877b0e5a50eeadc7))
* **connector:** add Authorizer interface and support for DSQL authorization ([85dd4f1](https://github.com/pgx-contrib/pgxaws/commit/85dd4f1751ef028c7d53ae2abbccd7f83a285835))
* **connector:** add AWS DSQL IAM authentication support ([aec7504](https://github.com/pgx-contrib/pgxaws/commit/aec75045bc0dfee15ec7bd67a0098db9287934cf))
* **connector:** extract RDS IAM auth logic to RDSAuth struct ([e29e369](https://github.com/pgx-contrib/pgxaws/commit/e29e369d3e88a996bc0c852c1bd9bb7db13689da))
* improve code clarity and error handling in cacher and connector ([5ae1022](https://github.com/pgx-contrib/pgxaws/commit/5ae1022073fce64025c769cd33c06da7b27ab865))


### Bug Fixes

* address race conditions, S3 TTL semantics, and resource leaks ([0259575](https://github.com/pgx-contrib/pgxaws/commit/0259575f0a8d5ab542b665b2cbea5e2585bee9c5))
* **ci:** merge with the App token so main gets its CI run ([#484](https://github.com/pgx-contrib/pgxaws/issues/484)) ([a3e4dcc](https://github.com/pgx-contrib/pgxaws/commit/a3e4dcca80cf19c108fdb250c9cf26ab81eea1ff))
* **github:** correct action versions in update.yml ([af26f9d](https://github.com/pgx-contrib/pgxaws/commit/af26f9ddf0dcb66780d4838964c9cb1b2cadb49b))
* update CI badge link from ci.yaml to ci.yml ([c39fc1c](https://github.com/pgx-contrib/pgxaws/commit/c39fc1c3d427c8fbb0724ab7f0c19ede9276b6eb))
