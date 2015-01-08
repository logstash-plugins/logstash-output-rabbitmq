require "logstash/devutils/rspec/spec_helper"

require "logstash/pipeline"
require "logstash/outputs/rabbitmq"

describe LogStash::Outputs::RabbitMQ do

  describe "rabbitmq static key" do
    config <<-END
      input {
        generator {
          count => 1
        }
      }
      output {
        rabbitmq {
          host => "localhost"
          exchange_type => "topic"
          exchange => "foo"
          key => "bar"
        }
      }
    END

    it "should use defined key" do
      exchange = double("exchange")
      expect_any_instance_of(LogStash::Outputs::RabbitMQ).to receive(:connect).and_return(nil)
      expect_any_instance_of(LogStash::Outputs::RabbitMQ).to receive(:declare_exchange).and_return(exchange)

      expect(exchange).to receive(:publish).with(an_instance_of(String), {:routing_key => "bar", :properties => {:persistent => true}})

      # we need to set expectations before running the pipeline, this is why we cannot use the
      # "agent" spec construct here so we do it manually
      pipeline = LogStash::Pipeline.new(config)
      pipeline.run
    end

  end

  describe "rabbitmq key with dynamic field" do
    config <<-END
      input {
        generator {
          count => 1
          add_field => ["foo", "bar"]
        }
      }
      output {
        rabbitmq {
          host => "localhost"
          exchange_type => "topic"
          exchange => "foo"
          key => "%{foo}"
        }
      }
    END

    it "should populate the key with the content of the event foo field" do
      exchange = double("exchange")
      expect_any_instance_of(LogStash::Outputs::RabbitMQ).to receive(:connect).and_return(nil)
      expect_any_instance_of(LogStash::Outputs::RabbitMQ).to receive(:declare_exchange).and_return(exchange)

      expect(exchange).to receive(:publish).with(an_instance_of(String), {:routing_key => "bar", :properties => {:persistent => true}})

      # we need to set expectations before running the pipeline, this is why we cannot use the
      # "agent" spec construct here so we do it manually
      pipeline = LogStash::Pipeline.new(config)
      pipeline.run
    end

  end

  describe "fail over to multiple instances" do
    require "march_hare"

    config <<-END
      input {
        generator {
          count => 1
        }
      }
      output {
        rabbitmq {
          host => ["host1", "host2"]
          exchange_type => "topic"
          exchange => "foo"
          key => "bar"
          debug => true
        }
      }
    END

    it "should accept multiple hosts" do
      exchange = double("exchange")
      conn = double("conn")
      channel = double("channel")

      # Connect to host1
      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host1",
        :port => 5672,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_return(conn)
      expect(conn).to receive(:create_channel).and_return(channel)
      expect(channel).to receive(:exchange).with("foo", {
        :type => :topic,
        :durable => true
      }).and_return(exchange)

      expect(exchange).to receive(:publish).with(an_instance_of(String), {
        :routing_key => "bar",
        :properties => {:persistent => true}
      }).and_raise(MarchHare::Exception)

      # Failover to host2
      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host2",
        :port => 5672,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_return(conn)
      expect(conn).to receive(:create_channel).and_return(channel)
      expect(channel).to receive(:exchange).with("foo", {
        :type => :topic,
        :durable => true
      }).and_return(exchange)

      # Failover back to host1
      expect(exchange).to receive(:publish).with(an_instance_of(String), {
        :routing_key => "bar",
        :properties => {:persistent => true}
      }).and_raise(MarchHare::Exception)

      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host1",
        :port => 5672,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_return(conn)
      expect(conn).to receive(:create_channel).and_return(channel)
      expect(channel).to receive(:exchange).with("foo", {
        :type => :topic,
        :durable => true
      }).and_return(exchange)

      # Succeed and finish
      expect(exchange).to receive(:publish).with(an_instance_of(String), {
        :routing_key => "bar",
        :properties => {:persistent => true}
      })

      expect(conn).to receive(:open?).and_return(true)
      expect(conn).to receive(:close)

      # Run this scenerio
      pipeline = LogStash::Pipeline.new(config)
      pipeline.run
    end

    it "should failover upon failing to connect" do
      conn = double("conn")
      channel = double("channel")
      exchange = double("exchange")

      # Fail to connect to host1
      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host1",
        :port => 5672,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_raise(MarchHare::Exception)
      # Failover to host2
      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host2",
        :port => 5672,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_return(conn)
      expect(conn).to receive(:create_channel).and_return(channel)
      expect(channel).to receive(:exchange).with("foo", {
        :type => :topic,
        :durable => true
      }).and_return(exchange)

      # Publish and close
      expect(exchange).to receive(:publish).with(an_instance_of(String), {
        :routing_key => "bar",
        :properties => {:persistent => true}
      })

      expect(conn).to receive(:open?).and_return(true)
      expect(conn).to receive(:close)


      # Run this scenerio
      pipeline = LogStash::Pipeline.new(config)
      pipeline.run
    end

  end

  describe "failover to multiple ports" do
    config <<-END
      input {
        generator {
          count => 1
        }
      }
      output {
        rabbitmq {
          host => ["host1:2345", "host2:6789"]
          exchange_type => "topic"
          exchange => "foo"
          key => "bar"
          debug => true
        }
      }
    END

    it "should accept multiple hosts" do
      exchange = double("exchange")
      conn = double("conn")
      channel = double("channel")

      # Connect to host1 at 2345
      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host1",
        :port => 2345,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_return(conn)
      expect(conn).to receive(:create_channel).and_return(channel)
      expect(channel).to receive(:exchange).with("foo", {
        :type => :topic,
        :durable => true
      }).and_return(exchange)

      # Failover to host2 at 6789
      expect(exchange).to receive(:publish).with(an_instance_of(String), {
        :routing_key => "bar",
        :properties => {:persistent => true}
      }).and_raise(MarchHare::Exception)

      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host2",
        :port => 6789,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_return(conn)
      expect(conn).to receive(:create_channel).and_return(channel)
      expect(channel).to receive(:exchange).with("foo", {
        :type => :topic,
        :durable => true
      }).and_return(exchange)

      # Failover back to host1 at 2345
      expect(exchange).to receive(:publish).with(an_instance_of(String), {
        :routing_key => "bar",
        :properties => {:persistent => true}
      }).and_raise(MarchHare::Exception)

      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host1",
        :port => 2345,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_return(conn)
      expect(conn).to receive(:create_channel).and_return(channel)
      expect(channel).to receive(:exchange).with("foo", {
        :type => :topic,
        :durable => true
      }).and_return(exchange)


      # succeed and close
      expect(exchange).to receive(:publish).with(an_instance_of(String), {
        :routing_key => "bar",
        :properties => {:persistent => true}
      })

      expect(conn).to receive(:open?).and_return(true)
      expect(conn).to receive(:close)

      pipeline = LogStash::Pipeline.new(config)
      pipeline.run
    end

    it "should failover upon failing to connect" do
      conn = double("conn")
      channel = double("channel")
      exchange = double("exchange")

      # Fail to connect to host1 at 2345
      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host1",
        :port => 2345,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_raise(MarchHare::Exception)
      # Connect to host2 at 6789
      expect(MarchHare).to receive(:connect).with({
        :vhost => "/",
        :host => "host2",
        :port => 6789,
        :user => "guest",
        :pass => "guest",
        :automatic_recovery => false
      }).and_return(conn)
      expect(conn).to receive(:create_channel).and_return(channel)
      expect(channel).to receive(:exchange).with("foo", {
        :type => :topic,
        :durable => true
      }).and_return(exchange)

      # Publish and close
      expect(exchange).to receive(:publish).with(an_instance_of(String), {
        :routing_key => "bar",
        :properties => {:persistent => true}
      })

      expect(conn).to receive(:open?).and_return(true)
      expect(conn).to receive(:close)


      # Run this scenerio
      pipeline = LogStash::Pipeline.new(config)
      pipeline.run
    end


  end

end
