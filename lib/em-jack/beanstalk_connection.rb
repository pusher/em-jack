require 'eventmachine'

module EMJack
  class BeanstalkConnection < EM::Connection
    attr_accessor :client

    def connection_completed
      @client.connected
    end

    def receive_data(data)
      @client.received(data)
    end

    def send(command, *args)
      argss = args.empty? ? "" : " #{args.join(" ")}"
      send_data("#{command.to_s}#{argss}\r\n")
    end

    def send_with_data(command, data, *args)
      send_data("#{command.to_s} #{args.join(" ")}\r\n#{data}\r\n")
    end

    def unbind
      @client.disconnected
    end
  end
end
