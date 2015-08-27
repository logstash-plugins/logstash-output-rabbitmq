* 2.0.0
  - Massive refactor
  - Implement Logstash 2.x stop behavior
  - Depend on rabbitmq_connection mixin for most connection functionality
* 1.1.2
 - Bump march_hare to 2.12.0 to fix jar file permissions
* 1.1.1
 - Fix nasty bug that caused connection duplication on conn recovery
* 1.1.0
 - Many internal refactors
 - Bump march hare version to 2.11.0
* 1.0.1
 - Fix connection leakage/failure to reconnect on disconnect