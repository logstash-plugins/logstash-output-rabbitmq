# encoding: UTF-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/pipeline"
require "logstash/outputs/rabbitmq"

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

  it "should register as an output plugin" do
    expect(LogStash::Plugin.lookup("output", "rabbitmq")).to eql(LogStash::Outputs::RabbitMQ)
  end

  it "should default @batching_enabled to false" do
    expect( instance.instance_variable_get(:@batching_enabled)).to be_falsey
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

      context "when a MarchHare::Exception is encountered" do
        before do
          i = 0
          allow(instance).to receive(:connect!)
          allow(instance).to receive(:sleep_for_retry)
          allow(exchange).to receive(:publish).with(any_args) do
            i += 1
            raise(MarchHare::Exception, "foo") if i == 1
          end

          instance.send(:publish, event, encoded_event)
        end

        it "should execute publish twice due to a retry" do
          expect(exchange).to have_received(:publish).twice
        end

        it "should sleep for the retry" do
          expect(instance).to have_received(:sleep_for_retry).once
        end

        it "should send the correct message (twice)" do
          expect(exchange).to have_received(:publish).with(encoded_event, anything).twice
        end

        it "should send the correct metadata (twice)" do
          expected_metadata = {:routing_key => event.sprintf(key), :properties => {:persistent => persistent }}

          expect(exchange).to have_received(:publish).with(anything, expected_metadata).twice
        end
      end
    end
  end

  context "batching not enabled, flush and do_close on the codec should not be called" do
    let(:event) { LogStash::Event.new("foo" => "bar") }
    let(:codec) { double("Codec") }

    before do
      instance.instance_variable_set(:@codec, codec)
      allow(codec).to receive(:encode)
      allow(codec).to receive(:flush)
      allow(codec).to receive(:do_close)
    end

    describe "#receive" do

      before do
        instance.receive( event)
      end

      it "should have called the codec to encode the data" do
        expect(codec).to have_received(:encode)
      end

      it "should not have called flush" do
        expect(codec).to have_received(:flush).exactly(0).times
      end
    end

    describe "#close" do
      before do
        instance.close()
      end

      it "should not have called the do_close on the codec" do
        expect(codec).to have_received(:do_close).exactly(0).times
      end
    end

    describe "#multi_receive" do
     let(:events) {
       [ LogStash::Event.new("foo" => "bar"),
         LogStash::Event.new("foo1" => "bar2")
       ]
      }
      before do
        instance.multi_receive( events)
      end

      it "should have called the codec to encode the data for each event" do
        expect(codec).to have_received(:encode).exactly(2).times
      end
      it "should have not have called flush" do
        expect(codec).to have_received(:flush).exactly(0).times
      end
    end
  end

  context "batching enabled, flush and do_close should be called on the codec" do
    let(:event) { LogStash::Event.new("foo" => "bar") }
    let(:codec) { double("Codec") }

    before do
      instance.instance_variable_set(:@codec, codec)
      instance.instance_variable_set(:@batching_enabled, true)
      allow(codec).to receive(:encode)
      allow(codec).to receive(:flush)
      allow(codec).to receive(:do_close)
    end

    describe "#receive" do

      before do
        instance.receive( event)
      end

      it "should have called the codec to encode the data" do
        expect(codec).to have_received(:encode)
      end

      it "should not have called flush" do
        expect(codec).to have_received(:flush).exactly(0).times
      end
    end

    describe "#close" do
      before do
        instance.close()
      end

      it "should have called the do_close on the codec" do
        expect(codec).to have_received(:do_close).exactly(1).times
      end
    end

    describe "#multi_receive" do
      let(:batch01) {
        [ LogStash::Event.new("foo0" => "bar0"),
          LogStash::Event.new("foo1" => "bar1")
        ]
       }
      let(:batch02) {
        [ LogStash::Event.new("foo2" => "bar2"),
          LogStash::Event.new("foo3" => "bar3")
        ]
       }
       before do
         instance.multi_receive( batch01)
         instance.multi_receive( batch02)
       end

       it "should have called the codec to encode the data for each event" do
         expect(codec).to have_received(:encode).exactly(4).times
       end
       it "should have called flush after each batch" do
         expect(codec).to have_received(:flush).exactly(2).times
       end
     end
  end
end


describe "with a live server", :integration => true do
  let(:klass) { LogStash::Outputs::RabbitMQ }
  let(:exchange) { "myexchange" }
  let(:exchange_type) { "topic" }
  let(:default_plugin_config) {
    {
      "host" => "127.0.0.1",
      "exchange" => exchange,
      "exchange_type" => exchange_type,
      "key" => "foo"
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
  end

  describe "sending a message with an exchange specified" do
    let(:message) { LogStash::Event.new("message" => "Foo Message", "extra_field" => "Blah") }

    before do
      @received = nil
      test_queue.subscribe do |metadata,payload|
        @received = payload
      end

      instance.receive(message)

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
