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

  # TODO: Redo all this
  # This is a crazy test to fix an urgent bug. It tests that we actually to change the
  # store exchange value when an exception happens
  # This should be a small number of lines, but until we refactor we must jump through hoops
  # to put the plugin in the proper state and extract the proper state to test
  describe "retrying a publish" do
    let(:settings) {
      {
        "host" => "localhost",
        "exchange_type" => "topic",
        "exchange" => "foo",
        "key" => "%{foo}"
      }
    }
    let(:event) { LogStash::Event.new("foo" => "bar")}

    subject { LogStash::Outputs::RabbitMQ.new(settings) }

    before do
      allow(subject).to receive(:connect) {
                          subject.instance_variable_get(:@connected).set(true)
                        }

      exchange_invocation = 0

      @most_recent_exchange = nil

      allow(subject).to receive(:declare_exchange) do
        exchange_invocation += 1
        @most_recent_exchange = double("exchange #{exchange_invocation}")
      end

      subject.register
    end

    context "when the connection aborts once" do
      before do
        i = 0
        allow(subject).to receive(:attempt_publish_serialized) do
          i+=1
          if i == 1 # Raise an exception once
           raise MarchHare::Exception, "First"
          else
            "nope"
          end
        end

        subject.publish_serialized(event, "hello")
      end

      it "should re-declare the exchange" do
        expect(subject).to have_received(:declare_exchange).twice
      end

      it "should set the exchange to the second double" do
        expect(subject.instance_variable_get(:@x)).to eql(@most_recent_exchange)
      end
    end
  end
end
