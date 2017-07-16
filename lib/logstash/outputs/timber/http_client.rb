# encoding: utf-8
require "logstash/config/mixin"
require "manticore"

# This contains the HTTP client code within it's own namespace. It is based off
# of the logstash-mixin-http_client plugin.
module LogStash::Outputs::Timber::HttpClient
  class InvalidHTTPConfigError < StandardError; end

  def self.included(base)
    base.extend(self)
    base.setup_http_client_config
  end

  def setup_http_client_config
    # Timeout (in seconds) for the entire request
    config :request_timeout, :validate => :number, :default => 60

    # Timeout (in seconds) to wait for data on the socket. Default is `10s`
    config :socket_timeout, :validate => :number, :default => 10

    # Timeout (in seconds) to wait for a connection to be established. Default is `10s`
    config :connect_timeout, :validate => :number, :default => 10

    # Max number of concurrent connections. Defaults to `50`
    config :pool_max, :validate => :number, :default => 50

    # If you need to use a custom X.509 CA (.pem certs) specify the path to that here
    config :cacert, :validate => :path

    # If you'd like to use a client certificate (note, most people don't want this) set the path to the x509 cert here
    config :client_cert, :validate => :path

    # If you're using a client certificate specify the path to the encryption key here
    config :client_key, :validate => :path

    # If you need to use a custom keystore (`.jks`) specify that here. This does not work with .pem keys!
    config :keystore, :validate => :path

    # Specify the keystore password here.
    # Note, most .jks files created with keytool require a password!
    config :keystore_password, :validate => :password

    # Specify the keystore type here. One of `JKS` or `PKCS12`. Default is `JKS`
    config :keystore_type, :validate => :string, :default => "JKS"

    # If you need to use a custom truststore (`.jks`) specify that here. This does not work with .pem certs!
    config :truststore, :validate => :path

    # Specify the truststore password here.
    # Note, most .jks files created with keytool require a password!
    config :truststore_password, :validate => :password

    # Specify the truststore type here. One of `JKS` or `PKCS12`. Default is `JKS`
    config :truststore_type, :validate => :string, :default => "JKS"

    # If you'd like to use an HTTP proxy . This supports multiple configuration syntaxes:
    #
    # 1. Proxy host in form: `http://proxy.org:1234`
    # 2. Proxy host in form: `{host => "proxy.org", port => 80, scheme => 'http', user => 'username@host', password => 'password'}`
    # 3. Proxy host in form: `{url =>  'http://proxy.org:1234', user => 'username@host', password => 'password'}`
    config :proxy
  end

  def client_config
    c = {
      connect_timeout: @connect_timeout,
      socket_timeout: @socket_timeout,
      request_timeout: @request_timeout,
      follow_redirects: true,
      automatic_retries: 1,
      retry_non_idempotent: true,
      check_connection_timeout: 200,
      pool_max: @pool_max,
      pool_max_per_route: @pool_max,
      cookies: false,
      keepalive: true
    }

    if @proxy
      # Symbolize keys if necessary
      c[:proxy] = @proxy.is_a?(Hash) ?
        @proxy.reduce({}) {|memo,(k,v)| memo[k.to_sym] = v; memo} :
        @proxy
    end

    c[:ssl] = {}
    if @cacert
      c[:ssl][:ca_file] = @cacert
    end

    if @truststore
      c[:ssl].merge!(
        :truststore => @truststore,
        :truststore_type => @truststore_type,
        :truststore_password => @truststore_password.value
      )

      if c[:ssl][:truststore_password].nil?
        raise LogStash::ConfigurationError, "Truststore declared without a password! This is not valid, please set the 'truststore_password' option"
      end
    end

    if @keystore
      c[:ssl].merge!(
        :keystore => @keystore,
        :keystore_type => @keystore_type,
        :keystore_password => @keystore_password.value
      )

      if c[:ssl][:keystore_password].nil?
        raise LogStash::ConfigurationError, "Keystore declared without a password! This is not valid, please set the 'keystore_password' option"
      end
    end

    if @client_cert && @client_key
      c[:ssl][:client_cert] = @client_cert
      c[:ssl][:client_key] = @client_key
    elsif !!@client_cert ^ !!@client_key
      raise InvalidHTTPConfigError, "You must specify both client_cert and client_key for an HTTP client, or neither!"
    end

    c
  end

  def http_client
    @http_client ||= Manticore::Client.new(client_config)
  end
end