# opensearch plugin (stubs)

Every method raises `NotImplementedError`, and `detect` returns `False`. The intended design:

- One container, so `default_parallel = 1` and one job.
- An image built from a Dockerfile: Ubuntu 24.04 plus the JDK from `.ci/java-versions.properties`.
- The job runs `./gradlew check -Dtests.coverage=true`.
- A docker volume for the Gradle cache, shared between runs.
- A Gradle init script that writes per-test events, one `<status>\t<class>\t<method>` line each, for `in_progress_results`.
- `collect_results` copies `**/build/test-results/**/*.xml`.
