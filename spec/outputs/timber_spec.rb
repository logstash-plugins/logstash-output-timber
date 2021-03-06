require "json"
require "thread"

require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/timber"
require "logstash/codecs/plain"
require "sinatra"

PORT = rand(65535-1024) + 1025

class LogStash::Outputs::Timber
  attr_writer :agent
  attr_reader :request_tokens
end

# note that Sinatra startup and shutdown messages are directly logged to stderr so
# it is not really possible to disable them without reopening stderr which is not advisable.
#
# == Sinatra (v1.4.6) has taken the stage on 51572 for development with backup from WEBrick
# == Sinatra has ended his set (crowd applauds)

class TestApp < Sinatra::Base
  # disable WEBrick logging
  def self.server_settings
    { :AccessLog => [], :Logger => WEBrick::BasicLog::new(nil, WEBrick::BasicLog::FATAL) }
  end

  def self.add_request(request)
    self.requests << request
  end

  def self.requests
    @requests ||= []
  end

  def self.reset_requests
    @requests = []
  end

  post "/good" do
    self.class.add_request(request)
    [200, "Good"]
  end

  post "/auth_error" do
    self.class.add_request(request)
    [403, "Bad"]
  end

  post "/server_error" do
    self.class.add_request(request)
    [500, "Bad"]
  end
end

RSpec.configure do |config|
  #http://stackoverflow.com/questions/6557079/start-and-call-ruby-http-server-in-the-same-script
  def sinatra_run_wait(app, opts)
    queue = Queue.new

    t = java.lang.Thread.new(
      proc do
        begin
          app.run!(opts) do |server|
            queue.push("started")
          end
        rescue => e
          puts "Error in webserver thread #{e}"
          # ignore
        end
      end
    )
    t.daemon = true
    t.start
    queue.pop # blocks until the run! callback runs
  end

  config.before(:suite) do
    sinatra_run_wait(TestApp, :port => PORT, :server => 'webrick')
    puts "Test webserver on port #{PORT}"
  end
end

describe LogStash::Outputs::Timber do
  let(:port) { PORT }
  let(:output) { described_class.new({"api_key" => "123:abcd1234", "pool_max" => 1}) }
  let(:event) { LogStash::Event.new({"message" => "hi"}) }
  let(:requests) { TestApp.requests }

  before(:each) do
    output.register
    TestApp.reset_requests
  end

  describe "#send_events" do
    it "returns false when the max attempts are exceeded" do
      result = output.send(:send_events, [event], 6)
      expect(result).to eq(false)
    end

    it "returns false when the status is 403" do
      output.url = "http://localhost:#{port}/auth_error"
      result = output.send(:send_events, [event], 1)
      expect(result).to eq(false)
      expect(requests.length).to eq(1)
    end

    it "returns false when the status is 500" do
      allow(output).to receive(:sleep_for_attempt).and_return(0)
      output.url = "http://localhost:#{port}/server_error"
      result = output.send(:send_events, [event], 1)
      expect(result).to eq(false)
      expect(requests.length).to eq(3)
    end

    it "handles fatal request errors" do
      allow(output.send(:http_client)).to receive(:post).and_raise("boom")

      output.url = "http://localhost:#{port}/good"
      result = output.send(:send_events, [event], 1)
      expect(result).to eq(false)
    end

    it "handles retryable request errors" do
      expect(output.send(:http_client)).to receive(:post).exactly(3).times.and_raise(::Manticore::Timeout.new)

      output.url = "http://localhost:#{port}/good"
      result = output.send(:send_events, [event], 1)
      expect(result).to eq(false)
    end

    it "returns true when the status is 200" do
      output.url = "http://localhost:#{port}/good"
      result = output.send(:send_events, [event], 1)
      expect(result).to eq(true)
      expect(requests.length).to eq(1)

      request = requests.first
      expect(request.env["CONTENT_TYPE"]).to eq("application/json")
      expect(request.env["HTTP_AUTHORIZATION"]).to eq("Basic MTIzOmFiY2QxMjM0")
      expect(request.env["HTTP_USER_AGENT"]).to eq("Timber Logstash/1.0.2")

      parsed_body = JSON.parse!(request.body.read)
      expect(parsed_body.length).to eq(1)

      body_event = parsed_body.first
      timestamp_iso8601 = event.get("@timestamp").to_iso8601
      expected_payload = {"$schema"=>"https://raw.githubusercontent.com/timberio/log-event-json-schema/v3.1.1/schema.json", "dt"=>timestamp_iso8601, "message"=>"hi"}
      expect(body_event).to eq(expected_payload)
    end
  end

  describe "#event_hash" do
    it "merges the timber key" do
      event = LogStash::Event.new({"message" => "my message", "timber" => {"context" => {"system" => {"pid" => 123}}}})
      hash = output.send(:event_hash, event)

      dt = event.get("@timestamp").utc.to_iso8601
      expect(hash).to eq({
        "$schema" => "https://raw.githubusercontent.com/timberio/log-event-json-schema/v3.1.1/schema.json",
        "context" => {"system" => {"pid" => 123}},
        "message" => "my message",
        "dt" => dt
      })
    end

    it "moves host" do
      event = LogStash::Event.new({"message" => "my message", "host" => "local.myhost.com"})
      hash = output.send(:event_hash, event)

      dt = event.get("@timestamp").utc.to_iso8601
      expect(hash).to eq({
        "$schema" => "https://raw.githubusercontent.com/timberio/log-event-json-schema/v3.1.1/schema.json",
        "message" => "my message",
        "dt" => dt,
        "context" => {"system" => {"hostname" => "local.myhost.com"}}
      })
    end

    it "moves everything else to meta" do
      event = LogStash::Event.new({"message" => "my message", "key" => "val"})
      hash = output.send(:event_hash, event)

      dt = event.get("@timestamp").utc.to_iso8601
      expect(hash).to eq({
        "$schema" => "https://raw.githubusercontent.com/timberio/log-event-json-schema/v3.1.1/schema.json",
        "message" => "my message",
        "dt" => dt,
        "meta" => {"key" => "val"}
      })
    end

    it "handles bother timber and host" do
      event = LogStash::Event.new({"message" => "my message", "host" => "local.myhost.com", "timber" => {"context" => {"system" => {"pid" => 123}}}})
      hash = output.send(:event_hash, event)

      dt = event.get("@timestamp").utc.to_iso8601
      expect(hash).to eq({
        "$schema" => "https://raw.githubusercontent.com/timberio/log-event-json-schema/v3.1.1/schema.json",
        "message" => "my message",
        "dt" => dt,
        "context" => {"system" => {"hostname" => "local.myhost.com", "pid" => 123}}
      })
    end
  end
end
