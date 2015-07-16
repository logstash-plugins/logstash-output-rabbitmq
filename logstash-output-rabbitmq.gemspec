Gem::Specification.new do |s|

  s.name            = 'logstash-output-rabbitmq'
  s.version         = '1.0.1'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "Push events to a RabbitMQ exchange"
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Elastic"]
  s.email           = 'info@elastic.co'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)+::Dir.glob('vendor/*')

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core", '>= 1.4.0', '< 2.0.0'

  if RUBY_PLATFORM == 'java'
    s.platform = RUBY_PLATFORM
    s.add_runtime_dependency 'march_hare', ['~> 2.5.1'] #(MIT license)
  else
    s.add_runtime_dependency 'bunny', ['>= 1.6.0'] #(MIT license)
  end

  s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency 'logstash-input-generator'
  s.add_development_dependency 'logstash-codec-json'
end

