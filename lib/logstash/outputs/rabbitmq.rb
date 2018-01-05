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
      
      concurrency :shared

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
        # The connection close should close all channels, so it is safe to store thread locals here without closing them
        @thread_local_channel = java.lang.ThreadLocal.new
        @thread_local_exchange = java.lang.ThreadLocal.new
      end

      def symbolize(myhash)
        Hash[myhash.map{|(k,v)| [k.to_sym,v]}]
      end

      def multi_receive_encoded(events_and_data)
        events_and_data.each do |event, data|
          publish(event, data)
        end
      end

      def publish(event, message)
        raise ArgumentError, "No exchange set in HareInfo!!!" unless @hare_info.exchange
        local_exchange.publish(message, :routing_key => event.sprintf(@key), :properties => symbolize(@message_properties.merge(:persistent => @persistent)))
      rescue MarchHare::Exception, IOError, AlreadyClosedException, TimeoutException => e
        @logger.error("Error while publishing. Will retry.",
                      :message => e.message,
                      :exception => e.class,
                      :backtrace => e.backtrace)

        sleep_for_retry
        retry
      end

      def local_exchange
        exchange = @thread_local_exchange.get
        if !exchange
          exchange = declare_exchange!(local_channel, @exchange, @exchange_type, @durable)
          @thread_local_exchange.set(exchange)
        end
        exchange
      end

      def local_channel
        channel = @thread_local_channel.get
        if !channel
          channel = @hare_info.connection.create_channel
          @thread_local_channel.set(channel)
        end
        channel
      end

      def close
        close_connection
      end
    end
  end
end