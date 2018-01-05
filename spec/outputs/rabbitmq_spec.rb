# encoding: UTF-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/pipeline"
require "logstash/outputs/rabbitmq"
java_import java.util.concurrent.TimeoutException

describe LogStash::Outputs::RabbitMQ do
  let(:klass) { LogStash::Outputs::RabbitMQ }
  let(:host) { "localhost" }
  let(:port) { 5672 }
  let(:exchange_type) { "topic" }
  let(:exchange) { "myexchange" }
  let(:key) { "mykey" }
  let(:persistent) { true }
  let(:rabbitmq_settings) {
    {
      "host" => host,
      "port" => port,
      "exchange_type" => exchange_type,
      "exchange" => exchange,
      "key" => key,
      "persistent" => persistent
    }
  }
  let(:instance) { klass.new(rabbitmq_settings) }
  let(:hare_info) { instance.instance_variable_get(:@hare_info) }

  shared_examples 'recovers from exception gracefully' do
    it 'should execute publish twice due to a retry' do
      expect(exchange).to have_received(:publish).twice
    end

    it 'should sleep for the retry' do
      expect(instance).to have_received(:sleep_for_retry).once
    end

    it 'should send the correct message (twice)' do
      expect(exchange).to have_received(:publish).with(encoded_event, anything).twice
    end

    it 'should send the correct metadata (twice)' do
      expected_metadata = {:routing_key => event.sprintf(key), :properties => {:persistent => persistent }}
      expect(exchange).to have_received(:publish).with(anything, expected_metadata).twice
    end
  end

  it "should register as an output plugin" do
    expect(LogStash::Plugin.lookup("output", "rabbitmq")).to eql(LogStash::Outputs::RabbitMQ)
  end

  context "when connected" do
    let(:connection) { double("MarchHare Connection") }
    let(:channel) { double("Channel") }
    let(:exchange) { double("Exchange") }

    before do
      allow(instance).to receive(:connect!).and_call_original
      allow(::MarchHare).to receive(:connect).and_return(connection)
      allow(connection).to receive(:create_channel).and_return(channel)
      allow(connection).to receive(:on_blocked)
      allow(connection).to receive(:on_unblocked)
      allow(connection).to receive(:on_shutdown)
      allow(channel).to receive(:exchange).and_return(exchange)

      instance.register
    end

    describe "#connect!" do
      subject { hare_info }

      it "should set the exchange correctly" do
        expect(subject.exchange).to eql(exchange)
      end
    end

    describe "#publish_encoded" do
      let(:event) { LogStash::Event.new("foo" => "bar") }
      let(:sprinted_key) { double("sprinted key") }
      let(:encoded_event) { LogStash::Json.dump(event) }

      describe "issuing the publish" do
        before do
          allow(exchange).to receive(:publish).with(any_args)
          allow(event).to receive(:sprintf).with(key).and_return(sprinted_key)
          instance.send(:publish, event, encoded_event)
        end

        it "should send the correct message" do
          expect(exchange).to have_received(:publish).with(encoded_event, anything)
        end

        it "should send the correct metadata" do
          expected_metadata = {:routing_key => sprinted_key, :properties => {:persistent => persistent }}

          expect(exchange).to have_received(:publish).with(anything, expected_metadata)
        end
      end

      context 'when an exception is encountered' do
        let(:exception) { nil }

        before do
          i = 0
          allow(instance).to receive(:connect!)
          allow(instance).to receive(:sleep_for_retry)
          allow(exchange).to receive(:publish).with(any_args) do
            i += 1
            raise exception if i == 1
          end

          instance.send(:publish, event, encoded_event)
        end

        context 'when it is a MarchHare exception' do
          let(:exception) { MarchHare::Exception }
          it_behaves_like 'recovers from exception gracefully'
        end

        context 'when it is a MarchHare::ChannelAlreadyClosed' do
          let(:exception) { MarchHare::ChannelAlreadyClosed }
          it_behaves_like 'recovers from exception gracefully'
        end

        context 'when it is a TimeoutException' do
          let(:exception) { TimeoutException.new }
          it_behaves_like 'recovers from exception gracefully'
        end
      end
    end
  end
end


describe "with a live server", :integration => true do
  let(:klass) { LogStash::Outputs::RabbitMQ }
  let(:exchange) { "myexchange" }
  let(:exchange_type) { "topic" }
  let(:priority) { 34 }
  let(:default_plugin_config) {
    {
      "host" => "127.0.0.1",
      "exchange" => exchange,
      "exchange_type" => exchange_type,
      "key" => "foo",
      "message_properties" => {
          "priority" => priority
      }
    }
  }
  let(:config) { default_plugin_config }
  let(:instance) { klass.new(config) }
  let(:hare_info) { instance.instance_variable_get(:@hare_info) }

  # Spawn a connection in the bg and wait up (n) seconds
  def spawn_and_wait(instance)
    instance.register

    20.times do
      instance.connected? ? break : sleep(0.1)
    end

    # Extra time to make sure the output can attach
    sleep 1
  end
  let(:message) { LogStash::Event.new("message" => "Foo Message", "extra_field" => "Blah") }
  let(:encoded) { message.to_json }
  let(:test_connection) { MarchHare.connect(instance.send(:rabbitmq_settings)) }
  let(:test_channel) { test_connection.create_channel }
  let(:test_queue) {
    test_channel.queue("testq", :auto_delete => true).bind(exchange, :key => config["key"])
  }

  before do
    # Materialize the instance in the current thread to prevent dupes
    # If you use multiple threads with lazy evaluation weird stuff happens
    instance
    spawn_and_wait(instance)

    test_channel # Start up the test client as well
    test_queue
  end

  after do
    instance.close()
    test_channel.close
    test_connection.close
  end

  context "using defaults" do
    it "should start, connect, and stop cleanly" do
      expect(instance.connected?).to be_truthy
    end

    it "should close cleanly" do
      instance.close
      expect(instance.connected?).to be_falsey
    end

    it 'applies per message settings' do
      instance.multi_receive_encoded([[message, encoded]])
      sleep 1.0

      message, payload = test_queue.pop
      expect(message.properties.to_s).to include("priority=#{priority}")
    end
  end

  describe "sending a message with an exchange specified" do
    let(:message) { LogStash::Event.new("message" => "Foo Message", "extra_field" => "Blah") }

    before do
      @received = nil
      test_queue.subscribe do |metadata,payload|
        @received = payload
      end

      instance.multi_receive_encoded([[message, encoded]])

      until @received
        sleep 1
      end

      @decoded = LogStash::Json.load(@received)
    end

    it "should process the message fully using the default (JSON) codec" do
      expect(@decoded).to eql(LogStash::Json.load(LogStash::Json.dump(message)))
    end
  end
end
