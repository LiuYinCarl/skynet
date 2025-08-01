local driver = require "skynet.socketdriver"
local skynet = require "skynet"
local skynet_core = require "skynet.core"
local assert = assert

local BUFFER_LIMIT = 128 * 1024
local socket = {}	-- api
local socket_pool = setmetatable( -- store all socket object
	{},
	{ __gc = function(p)
		for id,v in pairs(p) do
			driver.close(id)
			p[id] = nil
		end
	end
	}
)

local socket_onclose = {}
local socket_message = {}

local function wakeup(s)
	local co = s.co
	if co then
		s.co = nil
		skynet.wakeup(co)
	end
end

local function pause_socket(s, size)
	if s.pause ~= nil then
		return
	end
	if size then
		skynet.error(string.format("Pause socket (%d) size : %d" , s.id, size))
	else
		skynet.error(string.format("Pause socket (%d)" , s.id))
	end
	driver.pause(s.id)
    s.pause = true
    -- 为什么 pause 之后需要 yield 呢
	skynet.yield()	-- there are subsequent socket messages in mqueue, maybe.
end

local function suspend(s)
	assert(not s.co)
	s.co = coroutine.running()
	if s.pause then
		skynet.error(string.format("Resume socket (%d)", s.id))
		driver.start(s.id)
		skynet.wait(s.co)
		s.pause = nil
	else
		skynet.wait(s.co)
	end
	-- wakeup closing corouting every time suspend,
	-- because socket.close() will wait last socket buffer operation before clear the buffer.
	if s.closing then
		skynet.wakeup(s.closing)
	end
end

-- read skynet_socket.h for these macro
-- SKYNET_SOCKET_TYPE_DATA = 1
socket_message[1] = function(id, size, data)
	local s = socket_pool[id]
	if s == nil then
		skynet.error("socket: drop package from " .. id)
		driver.drop(data, size)
		return
	end

	local sz = driver.push(s.buffer, s.pool, data, size)
	local rr = s.read_required
	local rrt = type(rr)
	if rrt == "number" then
		-- read size
		if sz >= rr then
			s.read_required = nil
			if sz > BUFFER_LIMIT then
				pause_socket(s, sz)
			end
			wakeup(s)
		end
	else
		if s.buffer_limit and sz > s.buffer_limit then
			skynet.error(string.format("socket buffer overflow: fd=%d size=%d", id , sz))
			driver.close(id)
			return
		end
		if rrt == "string" then
			-- read line
			if driver.readline(s.buffer,nil,rr) then
				s.read_required = nil
				if sz > BUFFER_LIMIT then
					pause_socket(s, sz)
				end
				wakeup(s)
			end
		elseif sz > BUFFER_LIMIT and not s.pause then
			pause_socket(s, sz)
		end
	end
end

-- SKYNET_SOCKET_TYPE_CONNECT = 2
socket_message[2] = function(id, ud , addr)
	local s = socket_pool[id]
	if s == nil then
		return
	end
	-- log remote addr
	if not s.connected then	-- resume may also post connect message
		if s.listen then
			s.addr = addr
			s.port = ud
		end
		s.connected = true
		wakeup(s)
	end
end

-- SKYNET_SOCKET_TYPE_CLOSE = 3
socket_message[3] = function(id)
	local s = socket_pool[id]
	if s then
		s.connected = false
		wakeup(s)
	else
		driver.close(id)
	end
	local cb = socket_onclose[id]
	if cb then
		cb(id)
		socket_onclose[id] = nil
	end
end

-- SKYNET_SOCKET_TYPE_ACCEPT = 4
socket_message[4] = function(id, newid, addr)
	local s = socket_pool[id]
	if s == nil then
		driver.close(newid)
		return
	end
	s.callback(newid, addr)
end

-- SKYNET_SOCKET_TYPE_ERROR = 5
socket_message[5] = function(id, _, err)
	local s = socket_pool[id]
	if s == nil then
		driver.shutdown(id)
		skynet.error("socket: error on unknown", id, err)
		return
	end
	if s.callback then
		skynet.error("socket: accept error:", err)
		return
	end
	if s.connected then
		skynet.error("socket: error on", id, err)
	elseif s.connecting then
		s.connecting = err
	end
	s.connected = false
	driver.shutdown(id)

	wakeup(s)
end

-- SKYNET_SOCKET_TYPE_UDP = 6
socket_message[6] = function(id, size, data, address)
	local s = socket_pool[id]
	if s == nil or s.callback == nil then
		skynet.error("socket: drop udp package from " .. id)
		driver.drop(data, size)
		return
	end
	local str = skynet.tostring(data, size)
	skynet_core.trash(data, size)
	s.callback(str, address)
end

local function default_warning(id, size)
	local s = socket_pool[id]
	if not s then
		return
	end
	skynet.error(string.format("WARNING: %d K bytes need to send out (fd = %d)", size, id))
end

-- SKYNET_SOCKET_TYPE_WARNING
socket_message[7] = function(id, size)
	local s = socket_pool[id]
	if s then
		local warning = s.on_warning or default_warning
		warning(id, size)
	end
end

skynet.register_protocol {
	name = "socket",
	id = skynet.PTYPE_SOCKET,	-- PTYPE_SOCKET = 6
	unpack = driver.unpack,
	dispatch = function (_, _, t, ...)
		socket_message[t](...)
	end
}

local function connect(id, func)
	local newbuffer
	if func == nil then
		newbuffer = driver.buffer()
	end
	local s = {
		id = id,
		buffer = newbuffer,
		pool = newbuffer and {},
		connected = false,
		connecting = true,
		read_required = false,
		co = false,
		callback = func,
		protocol = "TCP",
	}
	assert(not socket_onclose[id], "socket has onclose callback")
	local s2 = socket_pool[id]
	if s2 and not s2.listen then
		error("socket is not closed")
	end
	socket_pool[id] = s
	suspend(s)
	local err = s.connecting
	s.connecting = nil
	if s.connected then
		return id
	else
		socket_pool[id] = nil
		return nil, err
	end
end

function socket.open(addr, port)
	local id = driver.connect(addr,port)
	return connect(id)
end

function socket.bind(os_fd)
	local id = driver.bind(os_fd)
	return connect(id)
end

function socket.stdin()
	return socket.bind(0)
end

function socket.start(id, func)
	driver.start(id)
	return connect(id, func)
end

function socket.pause(id)
	local s = socket_pool[id]
	if s == nil then
		return
	end
	pause_socket(s)
end

function socket.shutdown(id)
	local s = socket_pool[id]
	if s then
		-- the framework would send SKYNET_SOCKET_TYPE_CLOSE , need close(id) later
		driver.shutdown(id)
	end
end

function socket.close_fd(id)
	assert(socket_pool[id] == nil,"Use socket.close instead")
	driver.close(id)
end

function socket.close(id)
	local s = socket_pool[id]
	if s == nil then
		return
	end
	driver.close(id)
	if s.connected then
		s.pause = false -- Do not resume this fd if it paused.
		if s.co then
			-- reading this socket on another coroutine, so don't shutdown (clear the buffer) immediately
			-- wait reading coroutine read the buffer.
			assert(not s.closing)
			s.closing = coroutine.running()
			skynet.wait(s.closing)
		else
			suspend(s)
		end
		s.connected = false
	end
	socket_pool[id] = nil
end

function socket.read(id, sz)
	local s = socket_pool[id]
	assert(s)
	if sz == nil then
		-- read some bytes
		local ret = driver.readall(s.buffer, s.pool)
		if ret ~= "" then
			return ret
		end

		if not s.connected then
			return false, ret
		end
		assert(not s.read_required)
		s.read_required = 0
		suspend(s)
		ret = driver.readall(s.buffer, s.pool)
		if ret ~= "" then
			return ret
		else
			return false, ret
		end
	end

	local ret = driver.pop(s.buffer, s.pool, sz)
	if ret then
		return ret
	end
	if s.closing or not s.connected then
		return false, driver.readall(s.buffer, s.pool)
	end

	assert(not s.read_required)
	s.read_required = sz
	suspend(s)
	ret = driver.pop(s.buffer, s.pool, sz)
	if ret then
		return ret
	else
		return false, driver.readall(s.buffer, s.pool)
	end
end

function socket.readall(id)
	local s = socket_pool[id]
	assert(s)
	if not s.connected then
		local r = driver.readall(s.buffer, s.pool)
		return r ~= "" and r
	end
	assert(not s.read_required)
	s.read_required = true
	suspend(s)
	assert(s.connected == false)
	return driver.readall(s.buffer, s.pool)
end

function socket.readline(id, sep)
	sep = sep or "\n"
	local s = socket_pool[id]
	assert(s)
	local ret = driver.readline(s.buffer, s.pool, sep)
	if ret then
		return ret
	end
	if not s.connected then
		return false, driver.readall(s.buffer, s.pool)
	end
	assert(not s.read_required)
	s.read_required = sep
	suspend(s)
	if s.connected then
		return driver.readline(s.buffer, s.pool, sep)
	else
		return false, driver.readall(s.buffer, s.pool)
	end
end

function socket.block(id)
	local s = socket_pool[id]
	if not s or not s.connected then
		return false
	end
	assert(not s.read_required)
	s.read_required = 0
	suspend(s)
	return s.connected
end

socket.write = assert(driver.send)
socket.lwrite = assert(driver.lsend)
socket.header = assert(driver.header)

function socket.invalid(id)
	return socket_pool[id] == nil
end

function socket.disconnected(id)
	local s = socket_pool[id]
	if s then
		return not(s.connected or s.connecting)
	end
end

function socket.listen(host, port, backlog)
	if port == nil then
		host, port = string.match(host, "([^:]+):(.+)$")
		port = tonumber(port)
	end
	local id = driver.listen(host, port, backlog)
	local s = {
		id = id,
		connected = false,
		listen = true,
	}
	assert(socket_pool[id] == nil)
	socket_pool[id] = s
	suspend(s)
	return id, s.addr, s.port
end

-- abandon use to forward socket id to other service
-- you must call socket.start(id) later in other service
function socket.abandon(id)
	local s = socket_pool[id]
	if s then
		s.connected = false
		wakeup(s)
		socket_onclose[id] = nil
		socket_pool[id] = nil
	end
end

function socket.limit(id, limit)
	local s = assert(socket_pool[id])
	s.buffer_limit = limit
end

---------------------- UDP

local function create_udp_object(id, cb)
	assert(not socket_pool[id], "socket is not closed")
	socket_pool[id] = {
		id = id,
		connected = true,
		protocol = "UDP",
		callback = cb,
	}
end

function socket.udp(callback, host, port)
	local id = driver.udp(host, port)
	create_udp_object(id, callback)
	return id
end

function socket.udp_connect(id, addr, port, callback)
	local obj = socket_pool[id]
	if obj then
		assert(obj.protocol == "UDP")
		if callback then
			obj.callback = callback
		end
	else
		create_udp_object(id, callback)
	end
	driver.udp_connect(id, addr, port)
end

function socket.udp_listen(addr, port, callback)
	local id = driver.udp_listen(addr, port)
	create_udp_object(id, callback)
	return id
end

function socket.udp_dial(addr, port, callback)
	local id = driver.udp_dial(addr, port)
	create_udp_object(id, callback)
	return id
end

socket.sendto = assert(driver.udp_send)
socket.udp_address = assert(driver.udp_address)
socket.netstat = assert(driver.info)
socket.resolve = assert(driver.resolve)

function socket.warning(id, callback)
	local obj = socket_pool[id]
	assert(obj)
	obj.on_warning = callback
end

function socket.onclose(id, callback)
	socket_onclose[id] = callback
end

return socket
