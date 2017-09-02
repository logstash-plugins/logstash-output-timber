# encoding: utf-8

# Core classes
require "base64"
require "uri"

# Logstash classes
require "logstash/namespace"
require "logstash/json"
require "logstash/outputs/base"
require "logstash/outputs/timber/http_client"

# This is an output class that intelligently forwards logstash events to the Timber.io service.
#
# For a comprehensive overview around how this works and the various configuration options,
# please see: https://timber.io/docs/platforms/logstash
class LogStash::Outputs::Timber < LogStash::Outputs::Base
  include HttpClient

  VERSION = "1.0.2".freeze
  CONTENT_TYPE = "application/json".freeze
  JSON_SCHEMA = "https://raw.githubusercontent.com/timberio/log-event-json-schema/v3.1.1/schema.json".freeze
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
  URL = "https://ingestion-staging.timber.io/frames".freeze
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

    # This method takes a `Logstash::Event` object and converts it into a hash
    # that is acceptable by the Timber API. Each event is converted into a JSON
    # document that conforms to the Timber log event JSON schema:
    #
    # https://raw.githubusercontent.com/timberio/log-event-json-schema/v3.1.1/schema.json
    #
    # This realized by the following steps:
    #
    # 1. Timber will look for specific keys and map them to the appropriate keys as defined
    #    in our log event JSON schema. Specifically `@timestamp`, `host`, and `message`.
    # 2. If a `timber` key is present it _must_ be a hash that conforms to the Timber log event
    #    JSON schema. This hash will be merged in before being sent to Timber.
    # 3. All other root level keys will be treated as generic JSON and will be made available
    #    in Timber as they would kibana, etc.
    def event_hash(e)
      timber_hash = {"$schema" => JSON_SCHEMA}
      event_hash = e.to_hash

      # Delete unused logstash specific attributes
      event_hash.delete("@version")

      # Map the timber key first since we merge in values
      # later.
      timber = event_hash.delete("timber")
      if timber.is_a?(Hash)
        timber_hash.merge!(timber)
      end

      # Map the timestamp
      timestamp = event_hash.delete("@timestamp")

      if timestamp
        timber_hash["dt"] ||= timestamp.utc.to_iso8601
      end

      # Map the host
      host = event_hash.delete("host")

      if host
        timber_hash["context"] ||= {}
        timber_hash["context"]["system"] ||= {}
        timber_hash["context"]["system"]["hostname"] ||= host
      end

      # Map the message
      message = event_hash.delete("message")

      if message
        timber_hash["message"] ||= message
      end

      # Move everything else to meta, merging to preseve previous meta values.
      if event_hash != {}
        timber_hash["meta"] ||= {}
        timber_hash["meta"].merge!(event_hash)
      end

      timber_hash
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
