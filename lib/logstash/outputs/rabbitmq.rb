# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "march_hare"
require "java"

# Push events to a RabbitMQ exchange. Requires RabbitMQ 2.x
# or later version (3.x is recommended).
#
# Relevant links:
#
# * http://www.rabbitmq.com/[RabbitMQ]
# * http://rubymarchhare.info[March Hare]
class LogStash::Outputs::RabbitMQ < LogStash::Outputs::Base
  EXCHANGE_TYPES = ["fanout", "direct", "topic"]

  HareInfo = Struct.new(:connection, :channel, :exchange)

  config_name "rabbitmq"

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

  # Try to automatically recovery from broken connections. You almost certainly don't want to override this!!!
  config :automatic_recovery, :validate => :boolean, :default => true

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

  # Time in seconds to wait before retrying a connection
  config :connect_retry_interval, :validate => :number, :default => 1

  def initialize(params)
    params["codec"] = "json" if !params["codec"]

    super
  end

  public
  def register
    connect!
    @codec.on_event(&method(:publish))
  end

  public
  def receive(event)
    return unless output?(event)

    @codec.encode(event)
  rescue StandardError => e
    @logger.warn("Error encoding event", :exception => e, :event => event)
  end

  private
  def publish(event, message)
    @hare_info.exchange.publish(message, :routing_key => event.sprintf(@key), :properties => { :persistent => @persistent })
  rescue MarchHare::Exception, IOError, com.rabbitmq.client.AlreadyClosedException => e
    return if terminating?

    @logger.error("Error while publishing. Will retry.",
                  :message => e.message,
                  :exception => e.class,
                  :backtrace => e.backtrace)

    sleep_for_retry
    connect!
    retry
  end

  public
  def to_s
    return "<LogStash::RabbitMQ::Output: amqp://#{@user}@#{@host}:#{@port}#{@vhost}/#{@exchange_type}/#{@exchange}\##{@key}>"
  end

  public
  def teardown
    @hare_info.connection.close if connection_open?

    finished
  end

  private
  def settings
    return @settings if @settings

    s = {
      :vhost => @vhost,
      :host  => @host,
      :port  => @port,
      :user  => @user,
      :automatic_recovery => @automatic_recovery,
      :pass => @password ? @password.value : "guest",
    }
    s[:tls] = @ssl if @ssl
    @settings = s
  end

  private
  def connect
    @logger.debug("Connecting to RabbitMQ. Settings: #{settings.inspect}, queue: #{@queue.inspect}")


    connection = MarchHare.connect(settings)
    channel = connection.create_channel
    @logger.info("Connected to RabbitMQ at #{settings[:host]}")

    @logger.debug("Declaring an exchange", :name => @exchange,
                  :type => @exchange_type, :durable => @durable)

    exchange = channel.exchange(@exchange, :type => @exchange_type.to_sym, :durable => @durable)
    @logger.debug("Exchange declared")

    HareInfo.new(connection, channel, exchange)
  end

  private
  def connect!
    @hare_info = connect() unless connection_open?
  rescue MarchHare::Exception => e
    return if terminating?

    @logger.error("RabbitMQ connection error, will retry.",
                  :message => e.message,
                  :exception => e.class.name,
                  :backtrace => e.backtrace)

    sleep_for_retry
    retry
  end

  private
  def connection_open?
    @hare_info && @hare_info.connection && @hare_info.connection.open?
  end

  private
  def sleep_for_retry
    sleep @connect_retry_interval
  end
end