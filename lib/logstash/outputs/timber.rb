# encoding: utf-8

# Core classes
require "base64"
require "uri"

# Logstash classes
require "logstash/namespace"
require "logstash/json"
require "logstash/outputs/base"
require "logstash/outputs/timber/http_client"

class LogStash::Outputs::Timber < LogStash::Outputs::Base
  include HttpClient

  VERSION = "1.0.2".freeze
  CONTENT_TYPE = "application/json".freeze
  MAX_ATTEMPTS = 3
  METHOD = :post.freeze
  RETRYABLE_MANTICORE_EXCEPTIONS = [
    ::Manticore::Timeout,
    ::Manticore::SocketException,
    ::Manticore::ClientProtocolException,
    ::Manticore::ResolutionFailure,
    ::Manticore::SocketTimeout
  ].freeze
  RETRYABLE_CODES = [429, 500, 502, 503, 504].freeze
  URL = "https://logs.timber.io/frames".freeze
  USER_AGENT = "Timber Logstash/#{VERSION}".freeze

  # Attribute for testing purposes only
  attr_writer :url

  concurrency :shared

  # This output lets you send events to the Timber.io logging service.
  #
  # This output will execute up to 'pool_max' requests in parallel for performance.
  # Consider this when tuning this plugin for performance.
  #
  # Additionally, note that when parallel execution is used strict ordering of events is not
  # guaranteed!
  #
  # Beware, this gem does not yet support codecs. Please use the 'format' option for now.
  config_name "timber"

  # Your Timber API key. This can be obtained by creating an app at https://app.timber.io.
  # Already have an app? You can find your API key in your app's settings.
  config :api_key, :validate => :string, :required => :true

  def register
    encoded_api_key = Base64.urlsafe_encode64(@api_key).chomp
    authorization_value = "Basic #{encoded_api_key}"
    @headers = {
      "Authorization" => authorization_value,
      "Content-Type" => CONTENT_TYPE,
      "User-Agent" => USER_AGENT
    }
    @url = URL
  end

  def multi_receive(events)
    send_events(events, 1)
  end

  def close
    http_client.close
  end

  private
    def send_events(events, attempt)
      if attempt > MAX_ATTEMPTS
        @logger.warn(
          "Max attempts exceeded, dropping events",
          :attempt => attempt
        )
        return false
      end

      response =
        begin
          hash_events = events.collect { |e| event_hash(e) }
          body = LogStash::Json.dump(hash_events)
          http_client.post(@url, :body => body, :headers => @headers)
        rescue Exception => e
          if retryable_exception?(e)
            @logger.warn(
              "Attempt #{attempt}, retryable exception when making request",
              :attempt => attempt,
              :class => e.class.name,
              :message => e.message,
              :backtrace => e.backtrace
            )
            return send_events(events, attempt + 1)
          else
            @logger.error(
              "Attempt #{attempt}, fatal exception when making request",
              :attempt => attempt,
              :class => e.class.name,
              :message => e.message,
              :backtrace => e.backtrace
            )
            return false
          end
        end

      code = response.code

      if code >= 200 && code <= 299
        true
      elsif RETRYABLE_CODES.include?(code)
        @logger.warn(
          "Bad retryable response from the Timber API",
          :attempt => attempt,
          :code => code
        )
        sleep_time = sleep_for_attempt(attempt)
        sleep(sleep_time)
        send_events(events, attempt + 1)
      else
        @logger.error(
          "Bad fatal response from the Timber API",
          :attempt => attempt,
          :code => code
        )
        false
      end
    end

    def event_hash(e)
      hash = e.to_hash
      hash.delete("@version")
      dt = hash.delete("@timestamp")
      hash["dt"] = dt if !hash.has_key?("dt")
      hash
    end

    def retryable_exception?(e)
      RETRYABLE_MANTICORE_EXCEPTIONS.any? do |exception_class|
        e.is_a?(exception_class)
      end
    end

    def sleep_for_attempt(attempt)
      sleep_for = attempt ** 2
      sleep_for = sleep_for <= 60 ? sleep_for : 60
      (sleep_for / 2) + (rand(0..sleep_for) / 2)
    end
end
