# frozen_string_literal: true
# Usage: ruby ruby_control.rb SOCK_PATH ID TOKEN
#
# Contract fixture: sends a §9 control message — {"cmd":"restart","id":...,
# "token":...} as one NDJSON line — to the supervisor socket, exactly as
# Rails.supervisor.restart!(:id) will over IPC. Stdlib only.
require "socket"
require "json"

sock_path, id, token = ARGV
sock = UNIXSocket.new(sock_path)
sock.puts(JSON.generate(cmd: "restart", id: id, token: token))
sock.close
