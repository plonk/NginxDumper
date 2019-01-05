require 'ostruct'
require_relative 'util'
require 'socket'
require 'logger'
require 'shellwords'

class HttpFlvServer
  include Util

  class BadRequest < Exception; end

  SERVER_NAME = 'NginxDumper/0.0.1'

  def initialize(options = {})
    @host = options[:host] || '0.0.0.0'
    @port = options[:port] || 6000
    @log  = options[:logger] || Logger.new(STDOUT)
  end

  def accept_proc(s)
    # 切断した後では相手のアドレスが取れないので保存しておく。
    peeraddr = s.peeraddr

    handle_request(http_request(s))
    @log.info "done serving #{addr_format(peeraddr)}"
  rescue BadRequest => e
    @log.error e.message
    s.write "HTTP/1.0 400 Bad Request\r\n\r\n"
  rescue => e
    @log.error e.message
    e.backtrace.each do |line|
      @log.error line
    end
  ensure
    s.close
  end

  def run
    server_sock = TCPServer.open(@host, @port)

    @log.info "server started on #{addr_format(server_sock.addr)}"

    loop do
      read_ready, = IO.select([server_sock], [], [], 1.0)

      # defunct プロセスがたまらないように wait する。
      begin
        r = Process.wait(-1, Process::WNOHANG)
        if r
          @log.info("child #{r} ended")
        end
      rescue SystemCallError
        # no children
      end

      next unless read_ready

      client = server_sock.accept
      @log.info "connection accepted #{addr_format(client.peeraddr)}"

      pid = fork
      if pid.nil?
        begin
          @log.info "child #{Process.pid} started"
          accept_proc(client)
        ensure
          exit!(0)
        end
      else
        client.close
      end
    end
  rescue Interrupt
    @log.info 'interrupt from terminal'
  ensure
    if server_sock
      server_sock.close
    end
  end

  def http_request(s)
    if (line = s.gets) =~ /\A([A-Z]+) (\S+) (\S+)\r\n\z/
      meth = $1
      path = $2
      version = $3
    else
      fail BadRequest, "invalid request line: #{line.inspect}"
    end

    # read headers
    headers = {}
    while (line = s.gets) != "\r\n"
      if line =~ /\A([^:]+):\s*(.+)\r\n\z/
        if headers[$1]
          headers[$1] += ", #{$2}"
        else
          headers[$1] = $2
        end
      else
        fail BadRequest, "invalid header line: #{line.inspect}"
      end
    end
    OpenStruct.new(meth: meth, path: path, version: version,
                   headers: headers, socket: s)
  end

  def stats_body(request)
    "stats"
  end

  # 統計情報取得のリクエスト。
  def handle_stats(request)
    s = request.socket
    body = stats_body(request)

    s.write "HTTP/1.0 200 OK\r\n"
    s.write "Server: #{SERVER_NAME}\r\n"
    s.write "Content-Type: text/plain; charset=UTF-8\r\n"
    s.write "Content-Length: #{body.bytesize}\r\n"
    s.write "\r\n"

    s.write body
  ensure
    s.close
  end

  # 動画ストリーム取得のリクエスト。
  def handle_proxy(request)
    s = request.socket
    s.write "HTTP/1.0 200 OK\r\n"
    s.write "Server: #{SERVER_NAME}\r\n"
    s.write "Content-Type: video/x-flv\r\n"
    s.write "\r\n"
    s.flush

    source_path = "rtmp://localhost#{request.path}"
    command = "rtmpdump --live --timeout=20 --quiet -r #{Shellwords.escape source_path} -o -"

    exec command, out: s
  end

  # リクエストを種類によって振り分ける。
  def handle_request(request)
    @log.info("request: %s %s" % [request.meth, request.path])
    case request.meth
    when 'GET'
      if request.path == "/stats"
        handle_stats(request)
      else
        handle_proxy(request)
      end
    else
      request.socket "HTTP/1.0 400 Bad Request\r\n\r\n"
      request.socket.close
    end
  end
end


HttpFlvServer.new.run
