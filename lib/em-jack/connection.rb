require 'eventmachine'
require 'yaml'
require 'uri'

module EMJack
  class Connection
    include EM::Deferrable

    @@handlers = []

    attr_accessor :host, :port

    def self.register_handler(handler)
      @@handlers ||= []
      @@handlers << handler
    end

    def self.handlers
      @@handlers
    end

    def initialize(opts = {})
      case opts
      when Hash
        @host = opts[:host] || 'localhost'
        @port = opts[:port] || 11300
        @max_retries = opts[:max_retries] || 5
        @tube = opts[:tube]
      when String
        uri = URI.parse(opts)
        @host = uri.host || 'localhost'
        @port = uri.port || 11300
        @max_retries = 5
        @tube = uri.path.gsub(/^\//, '') # Kill the leading /
      end

      reset_tube_state

      @data = ""
      @retries = 0
      @in_reserve = false
      @fiberized = false

      @conn = EM::connect(host, port, EMJack::BeanstalkConnection) do |conn|
        conn.client = self
      end

      unless @tube.nil?
        @use_on_connect = @tube
        @watch_on_connect = [@tube]
        initialize_tube_state
      end

      @intentionally_closed = false
    end

    def initialize_tube_state
      if @use_on_connect
        @fiberized ? ause(@use_on_connect) : use(@use_on_connect)
      end

      [@watch_on_connect].flatten.compact.each do |tube|
        @fiberized ? awatch(tube) : watch(tube)
      end

      ignore = [@watched_tubes].flatten.compact - [@watch_on_connect].flatten.compact
      ignore.each { |tube| @fiberized ? aignore(tube) : ignore(tube) }
    end

    def reset_tube_state
      @use_on_connect ||= @used_tube
      @watch_on_connect ||= @watched_tubes.dup if @watched_tubes

      @used_tube = 'default'
      @watched_tubes = ['default']
      @deferrables = []
    end

    def fiber!
      @fiberized = true

      eigen = (class << self; self; end)
      eigen.instance_eval do
        %w(use reserve ignore watch peek stats list delete touch bury kick pause release put).each do |meth|
          alias_method :"a#{meth}", meth.to_sym
          define_method(meth.to_sym) do |*args|
            fib = Fiber.current
            ameth = :"a#{meth}"
            df = send(ameth, *args)
            if df
              df.callback { |*result| fib.resume(*result) }
              df.errback { fib.resume }
              Fiber.yield
            end
          end
        end
      end
    end

    def use(tube, &blk)
      return if @used_tube == tube

      callback {
        @used_tube = tube
        @conn.send(:use, tube)
      }

      add_deferrable(&blk)
    end

    def watch(tube, &blk)
      return if @watched_tubes.include?(tube)

      callback { @conn.send(:watch, tube) }

      df = add_deferrable(&blk)
      df.callback { @watched_tubes.push(tube) }
      df
    end

    def ignore(tube, &blk)
      return unless @watched_tubes.include?(tube)

      callback { @conn.send(:ignore, tube) }

      df = add_deferrable(&blk)
      df.callback { @watched_tubes.delete(tube) }
      df
    end

    def reserve(timeout = nil, &blk)
      callback {
        if timeout
          @conn.send(:'reserve-with-timeout', timeout)
        else
          @conn.send(:reserve)
        end
      }

      add_deferrable(&blk)
    end

    def peek(type = nil, &blk)
      callback {
        case(type.to_s)
        when /^\d+$/ then @conn.send(:peek, type)
        when "ready" then @conn.send(:'peek-ready')
        when "delayed" then @conn.send(:'peek-delayed')
        when "buried" then @conn.send(:'peek-buried')
        else raise EMJack::InvalidCommand.new
        end
      }

      add_deferrable(&blk)
    end

    def stats(type = nil, val = nil, &blk)
      callback {
        case(type)
        when nil then @conn.send(:stats)
        when :tube then @conn.send(:'stats-tube', val)
        when :job then @conn.send(:'stats-job', val.jobid)
        else raise EMJack::InvalidCommand.new
        end
      }

      add_deferrable(&blk)
    end

    def list(type = nil, &blk)
      callback {
        case(type)
        when nil then @conn.send(:'list-tubes')
        when :used then @conn.send(:'list-tube-used')
        when :watched then @conn.send(:'list-tubes-watched')
        else raise EMJack::InvalidCommand.new
        end
      }
      add_deferrable(&blk)
    end

    def delete(job, &blk)
      return if job.nil?

      callback { @conn.send(:delete, job.jobid) }

      add_deferrable(&blk)
    end

    def touch(job, &blk)
      return if job.nil?

      callback { @conn.send(:touch, job.jobid) }

      add_deferrable(&blk)
    end

    def bury(job, pri, &blk)
      callback { @conn.send(:bury, job.jobid, pri) }

      add_deferrable(&blk)
    end

    def kick(count = 1, &blk)
      callback { @conn.send(:kick, count) }

      add_deferrable(&blk)
    end

    def pause(tube, delay, &blk)
      callback { @conn.send(:'pause-tube', delay) }

      add_deferrable(&blk)
    end

    def release(job, opts = {}, &blk)
      return if job.nil?

      pri = (opts[:priority] || 65536).to_i
      delay = (opts[:delay] || 0).to_i

      callback { @conn.send(:release, job.jobid, pri, delay) }

      add_deferrable(&blk)
    end

    def put(msg, opts = nil, &blk)
      opts = {} if opts.nil?

      pri = (opts[:priority] || 65536).to_i
      pri = 65536 if pri< 0
      pri = 2 ** 32 if pri > (2 ** 32)

      delay = (opts[:delay] || 0).to_i
      delay = 0 if delay < 0

      ttr = (opts[:ttr] || 300).to_i
      ttr = 300 if ttr < 0

      m = msg.to_s

      callback { @conn.send_with_data(:put, m, pri, delay, ttr, m.bytesize) }

      add_deferrable(&blk)
    end

    def each_job(timeout = nil, &blk)
      if (@fiberized)
        work = Proc.new do
          job = reserve(timeout)
          blk.call(job) if job
          EM.next_tick { Fiber.new { work.call }.resume }
        end
      else
        work = Proc.new do
          r = reserve(timeout)
          r.callback do |job|
            blk.call(job)
            EM.next_tick { work.call }
          end
          r.errback do
            EM.next_tick { work.call }
          end
        end
      end
      work.call
    end

    def connected
      @reconnect_proc = nil
      @retries = 0
      succeed
      @connected = true
      @connected_callback.call if @connected_callback
      @use_on_connect = nil
      @watch_on_connect = nil

      @conn.close_connection_after_writing if @intentionally_closed
    end

    def disconnect
      @intentionally_closed = true
      @conn.close_connection_after_writing if connected?
      @close_df = EM::DefaultDeferrable.new
    end

    def disconnected
      @connected = false
      if @intentionally_closed
        @close_df.succeed
      else
        d = @deferrables.dup

        ## if reconnecting, need to fail ourself to remove any callbacks
        fail

        set_deferred_status(nil)
        d.each { |df| df.fail(:disconnected) }

        if @retries >= @max_retries
          if @disconnected_callback
            @disconnected_callback.call
          else
            raise EMJack::Disconnected
          end
        end

        reset_tube_state
        initialize_tube_state
        unless @reconnect_proc
          recon = Proc.new { @conn.reconnect(@host, @port) }
          if @fiberized
            @reconnect_proc = Proc.new { Fiber.new { recon.call }.resume }
          else
            @reconnect_proc = recon
          end
        end

        @retries += 1
        EM.add_timer(5) { @reconnect_proc.call }
      end
    end

    def reconnect!
      @retries = 0

      reset_tube_state
      EM.next_tick do
        @conn.reconnect(@host, @port)
        initialize_tube_state
      end
    end

    def add_deferrable(&blk)
      df = EM::DefaultDeferrable.new
      if @error_callback
        df.errback { |err| @error_callback.call(err) }
      end

      df.callback(&blk) if block_given?

      @deferrables.push(df)
      df
    end

    def on_error(&blk)
      @error_callback = blk
    end

    def on_disconnect(&blk)
      @disconnected_callback = blk
    end

    def on_connect(&blk)
      @connected_callback = blk
    end

    def connected?
      @connected
    end

    def received(data)
      @data << data

      wait = false
      while !wait && (idx = @data.index(/\r\n/))
        idx += 2 # Include the line terminator
        keyword = @data[0..idx]

        EMJack::Connection.handlers.each do |h|
          handles, needs_body = h.handles?(keyword)
          next unless handles

          if needs_body
            body_len = needs_body.to_i
            if @data.length >= idx + body_len + 2 # Remember the line terminator
              # Body is available
              body = @data.slice(idx, body_len)
              idx += body_len + 2
            else
              # We need to read more before we have all of the body
              wait = true
              break
            end
          end

          h.handle(@deferrables.shift, keyword, body, self)

          @data = @data[idx..-1] || ""
          break
        end
      end
    end

  end
end
