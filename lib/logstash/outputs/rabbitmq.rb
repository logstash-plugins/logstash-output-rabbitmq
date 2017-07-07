# encoding: UTF-8
require "logstash/pipeline"
require "logstash/plugin_mixins/rabbitmq_connection"
java_import java.util.concurrent.TimeoutException
java_import com.rabbitmq.client.AlreadyClosedException

# Push events to a RabbitMQ exchange. Requires RabbitMQ 2.x
# or later version (3.x is recommended).
# 
# Relevant links:
#
# * http://www.rabbitmq.com/[RabbitMQ]
# * http://rubymarchhare.info[March Hare]
module LogStash
  module Outputs
    class RabbitMQ < LogStash::Outputs::Base
      include LogStash::PluginMixins::RabbitMQConnection

      config_name "rabbitmq"

      # The default codec for this plugin is JSON. You can override this to suit your particular needs however.
      default :codec, "json"

      # Key to route to by default. Defaults to 'logstash'
      #
      # * Routing keys are ignored on fanout exchanges.
      config :key, :validate => :string, :default => "logstash"

      # The name of the exchange
      config :exchange, :validate => :string, :required => true

      # The exchange type (fanout, topic, direct)
      config :exchange_type, :validate => EXCHANGE_TYPES, :required => true

      # Is this exchange durable? (aka; Should it survive a broker restart?)
      config :durable, :validate => :boolean, :default => true

      # Should RabbitMQ persist messages to disk?
      config :persistent, :validate => :boolean, :default => true

      # Properties to be passed along with the message
      config :message_properties, :validate => :hash, :default => {}

      def register
        connect!
        @hare_info.exchange = declare_exchange!(@hare_info.channel, @exchange, @exchange_type, @durable)
        @codec.on_event(&method(:publish))
      end

      def symbolize(myhash)
        Hash[myhash.map{|(k,v)| [k.to_sym,v]}]
      end

      def receive(event)
        @codec.encode(event)
      rescue StandardError => e
        @logger.warn("Error encoding event", :exception => e, :event => event)
      end

      def publish(event, message)
        raise ArgumentError, "No exchange set in HareInfo!!!" unless @hare_info.exchange
        @hare_info.exchange.publish(message, :routing_key => event.sprintf(@key), :properties => symbolize(@message_properties.merge(:persistent => @persistent)))
      rescue MarchHare::Exception, IOError, AlreadyClosedException, TimeoutException => e
        @logger.error("Error while publishing. Will retry.",
                      :message => e.message,
                      :exception => e.class,
                      :backtrace => e.backtrace)

        sleep_for_retry
        retry
      end

      def close
        close_connection
      end
    end
  end
end