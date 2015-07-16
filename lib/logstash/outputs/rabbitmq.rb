# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# Push events to a RabbitMQ exchange. Requires RabbitMQ 2.x
# or later version (3.x is recommended).
#
# Relevant links:
#
# * http://www.rabbitmq.com/[RabbitMQ]
# * http://rubymarchhare.info[March Hare]
class LogStash::Outputs::RabbitMQ < LogStash::Outputs::Base
  EXCHANGE_TYPES = ["fanout", "direct", "topic"]

  config_name "rabbitmq"


  #
  # Connection
  #

  # RabbitMQ server address
  config :host, :validate => :string, :required => true

  # RabbitMQ port to connect on
  config :port, :validate => :number, :default => 5672

  # RabbitMQ username
  config :user, :validate => :string, :default => "guest"

  # RabbitMQ password
  config :password, :validate => :password, :default => "guest"

  # The vhost to use. If you don't know what this is, leave the default.
  config :vhost, :validate => :string, :default => "/"

  # Enable or disable SSL
  config :ssl, :validate => :boolean, :default => false

  # Validate SSL certificate
  config :verify_ssl, :validate => :boolean, :default => false

  # Enable or disable logging
  config :debug, :validate => :boolean, :default => false, :deprecated => "Use the logstash --debug flag for this instead."



  #
  # Exchange
  #


  # The exchange type (fanout, topic, direct)
  config :exchange_type, :validate => EXCHANGE_TYPES, :required => true

  # The name of the exchange
  config :exchange, :validate => :string, :required => true

  # Key to route to by default. Defaults to 'logstash'
  #
  # * Routing keys are ignored on fanout exchanges.
  config :key, :validate => :string, :default => "logstash"

  # Is this exchange durable? (aka; Should it survive a broker restart?)
  config :durable, :validate => :boolean, :default => true

  # Should RabbitMQ persist messages to disk?
  config :persistent, :validate => :boolean, :default => true



  def initialize(params)
    params["codec"] = "json" if !params["codec"]

    super
  end

  #
  # API
  #

  def register
    require "march_hare"
    require "java"

    @logger.info("Registering output", :plugin => self)

    @connected = java.util.concurrent.atomic.AtomicBoolean.new

    connect
    @x = declare_exchange

    @connected.set(true)

    @codec.on_event(&method(:publish_serialized))
  end


  def receive(event)
    return unless output?(event)

    begin
      @codec.encode(event)
    rescue => e
      @logger.warn("Error encoding event", :exception => e, :event => event)
    end
  end

  def publish_serialized(event, message)
    attempt_publish_serialized(event, message)
  rescue MarchHare::Exception, IOError, com.rabbitmq.client.AlreadyClosedException => e
    @connected.set(false)
    n = 10

    @logger.error("RabbitMQ connection error: #{e.message}. Will attempt to reconnect in #{n} seconds...",
                  :exception => e,
                  :backtrace => e.backtrace)
    return if terminating?

    sleep n

    connect
    @x = declare_exchange
    retry
  end

  def attempt_publish_serialized(event, message)
    if @connected.get
      @x.publish(message, :routing_key => event.sprintf(@key), :properties => { :persistent => @persistent })
    else
      @logger.warn("Tried to send a message, but not connected to RabbitMQ.")
    end
  end

  def to_s
    return "amqp://#{@user}@#{@host}:#{@port}#{@vhost}/#{@exchange_type}/#{@exchange}\##{@key}"
  end

  def teardown
    @connected.set(false)
    @conn.close if @conn && @conn.open?
    @conn = nil

    finished
  end



  #
  # Implementation
  #

  def connect
    return if terminating?

    @vhost       ||= "127.0.0.1"
    # 5672. Will be switched to 5671 by Bunny if TLS is enabled.
    @port        ||= 5672

    @settings = {
      :vhost => @vhost,
      :host  => @host,
      :port  => @port,
      :user  => @user,
      :automatic_recovery => false
    }
    @settings[:pass]      = if @password
                              @password.value
                            else
                              "guest"
                            end

    @settings[:tls]        = @ssl if @ssl
    proto                  = if @ssl
                               "amqp"
                             else
                               "amqps"
                             end
    @connection_url        = "#{proto}://#{@user}@#{@host}:#{@port}#{vhost}/#{@queue}"

    begin
      @conn = MarchHare.connect(@settings) unless @conn && @conn.open?

      @logger.debug("Connecting to RabbitMQ. Settings: #{@settings.inspect}, queue: #{@queue.inspect}")

      @ch = @conn.create_channel
      @logger.info("Connected to RabbitMQ at #{@settings[:host]}")
    rescue MarchHare::Exception => e
      @connected.set(false)
      n = 10

      @logger.error("RabbitMQ connection error: #{e.message}. Will attempt to reconnect in #{n} seconds...",
                    :exception => e,
                    :backtrace => e.backtrace)
      return if terminating?

      sleep n
      retry
    end
  end

  def declare_exchange
    @logger.debug("Declaring an exchange", :name => @exchange, :type => @exchange_type,
                  :durable => @durable)
    x = @ch.exchange(@exchange, :type => @exchange_type.to_sym, :durable => @durable)

    # sets @connected to true during recovery. MK.
    @connected.set(true)

    x
  end
end