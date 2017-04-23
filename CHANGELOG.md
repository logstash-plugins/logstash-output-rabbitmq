## 4.0.6
  - Bump rabbitmq connection to fix LongString type usage

## 4.0.5
  - Fix broken tests due to latest logstash-output-rabbitmq_connection

## 4.0.4
  - Depend on latest RMQ connection which retries on connection exception

## 4.0.3
  - Relax constraint on logstash-core-plugin-api to >= 1.60 <= 2.99

## 4.0.2
  - Depend on latest RMQ connection with TLS improvements

## 4.0.1
  - Republish all the gems under jruby.
## 4.0.0
  - Update the plugin to the version 2.0 of the plugin api, this change is required for Logstash 5.0 compatibility. See https://github.com/elastic/logstash/issues/5141
# 3.0.9
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash
# 3.0.8
  - New dependency requirements for logstash-core for the 5.0 release
## 3.0.7
  - Bump rabbitmq_connection version to use newer march hare gem and fix perms issues

## 3.0.6
 - Set codec to JSON if none specified to recreate old behavior

## 3.0.5
 - Fix broken registration of plugin

## 3.0.3
 - Bump dependency on logstash-mixin-rabbitmq_connection

## 3.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

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
