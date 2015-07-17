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
  let(:settings) {
    {
      "host" => host,
      "port" => port,
      "exchange_type" => exchange_type,
      "exchange" => exchange,
      "key" => key,
      "persistent" => persistent
    }
  }
  let(:instance) { klass.new(settings) }
  let(:hare_info) { instance.instance_variable_get(:@hare_info) }

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

    describe "#register" do
      subject { instance }

      it "should create cleanly" do
        expect(subject).to be_a(klass)
      end

      it "should connect" do
        expect(subject).to have_received(:connect!).once
      end
    end

    describe "#connect!" do
      subject { hare_info }

      it "should set @hare_info correctly" do
        expect(subject).to be_a(LogStash::Outputs::RabbitMQ::HareInfo)
      end

      it "should set @connection correctly" do
        expect(subject.connection).to eql(connection)
      end

      it "should set the channel correctly" do
        expect(subject.channel).to eql(channel)
      end

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

  # If the connection encounters an exception during its initial
  # connection attempt we must handle that. Subsequent errors should be
  # handled by the automatic retry mechanism built-in to MarchHare
  describe "initial connection exceptions" do
    subject { instance }

    before do
      allow(subject).to receive(:sleep_for_retry)


      i = 0
      allow(subject).to receive(:connect) do
        i += 1
        if i == 1
          raise(MarchHare::ConnectionRefused, "Error!")
        else
          double("connection")
        end
      end

      subject.send(:connect!)
    end

    it "should retry its connection when conn fails" do
      expect(subject).to have_received(:connect).twice
    end

    it "should sleep between retries" do
      expect(subject).to have_received(:sleep_for_retry).once
    end
  end
end
