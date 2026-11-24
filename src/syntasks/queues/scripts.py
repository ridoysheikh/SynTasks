"""Lua scripts of the queue engine (see engine.py for the key layout)."""

from syntasks.cluster.scripts import Script

GROUP = "workers"
WAKE_KEEP = 16

_HELPERS = """
local function ensure_group(stream, group)
  if redis.call('EXISTS', stream) == 0 then
    redis.call('XGROUP', 'CREATE', stream, group, '0', 'MKSTREAM')
  else
    redis.pcall('XGROUP', 'CREATE', stream, group, '0')
  end
end
local function wake(key)
  redis.call('LPUSH', key, '1')
  redis.call('LTRIM', key, 0, %d)
  redis.call('PEXPIRE', key, 60000)
end
local function field(fields, name)
  for i = 1, #fields, 2 do
    if fields[i] == name then return fields[i + 1] end
  end
  return false
end
""" % (WAKE_KEEP - 1)

# KEYS: 1 target stream, 2 queue streams set, 3 queue index, 4 wake list,
#       5 unique (or throttle) key, 6 delayed zset, 7 fair list of the key,
#       8 fair rotation list, 9 fair active set
# ARGV: 2 data, 3 task, 4 queue, 5 max_length|'', 6 on_full,
#       7 unique ttl ms|'', 8 task id, 9 delay ms|'', 10 group, 11 wake '1'|'',
#       12 delayed member prefix (32 chars), 13 fair key|
KICK = Script(
    "queue_kick",
    _HELPERS
    + """
if ARGV[7] ~= '' then
  local existing = redis.call('GET', KEYS[5])
  if existing then return {'dup', existing} end
end
if ARGV[5] ~= '' and ARGV[6] == 'reject' then
  local total = redis.call('ZCARD', KEYS[6])
  for _, s in ipairs(redis.call('SMEMBERS', KEYS[2])) do
    total = total + redis.call('XLEN', s)
  end
  if total >= tonumber(ARGV[5]) then return {'full', tostring(total)} end
end
if ARGV[7] ~= '' then
  redis.call('SET', KEYS[5], ARGV[8], 'PX', ARGV[7])
end
redis.call('SADD', KEYS[3], ARGV[4])
if redis.call('SADD', KEYS[2], KEYS[1]) == 1 or redis.call('EXISTS', KEYS[1]) == 0 then
  ensure_group(KEYS[1], ARGV[10])
end
if ARGV[9] ~= '' then
  local member = ARGV[12] .. string.format('%04d', #KEYS[1]) .. KEYS[1]
    .. string.format('%04d', #ARGV[3]) .. ARGV[3] .. ARGV[2]
  redis.call('ZADD', KEYS[6], now_ms() + tonumber(ARGV[9]), member)
  return {'delayed', ''}
end
if ARGV[13] ~= '' then
  redis.call('RPUSH', KEYS[7], ARGV[2])
  if redis.call('SADD', KEYS[9], ARGV[13]) == 1 then redis.call('RPUSH', KEYS[8], ARGV[13]) end
  if ARGV[11] == '1' then wake(KEYS[4]) end
  return {'ok', ''}
end
local id
if ARGV[5] ~= '' and ARGV[6] == 'drop_oldest' then
  id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[5], '*', 'data', ARGV[2], 'task', ARGV[3])
else
  id = redis.call('XADD', KEYS[1], '*', 'data', ARGV[2], 'task', ARGV[3])
end
if ARGV[11] == '1' then wake(KEYS[4]) end
return {'ok', id}
""",
)

# KEYS: 1 delayed zset
# ARGV: 2 batch, 3 group, 4 wake key prefix (…:lim:wake:task:)
# Returns {moved, next due ms or ''}.
MOVE_DELAYED = Script(
    "queue_move_delayed",
    _HELPERS
    + """
local now = now_ms()
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  local pos = 33
  local slen = tonumber(string.sub(member, pos, pos + 3))
  local stream = string.sub(member, pos + 4, pos + 3 + slen)
  pos = pos + 4 + slen
  local tlen = tonumber(string.sub(member, pos, pos + 3))
  local task = string.sub(member, pos + 4, pos + 3 + tlen)
  local data = string.sub(member, pos + 4 + tlen)
  ensure_group(stream, ARGV[3])
  redis.call('XADD', stream, '*', 'data', data, 'task', task)
  if string.find(stream, ':t:', 1, true) then
    wake(ARGV[4] .. task)
  end
end
local head = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local next_due = ''
if head[2] then next_due = head[2] end
return {#due, next_due}
""",
)

# Feed a fair task's stream from its per-key lists, one key at a time in
# rotation, so the stream never holds more than ``max`` undelivered entries.
# KEYS: 1 rotation list, 2 active set, 3 task stream
# ARGV: 2 list key prefix (…:fair:<task>:k:), 3 group, 4 task, 5 max undelivered
# Returns the number of entries fed.
FAIR_FEED = Script(
    "queue_fair_feed",
    _HELPERS
    + """
ensure_group(KEYS[3], ARGV[3])
local pending = redis.call('XPENDING', KEYS[3], ARGV[3])[1]
local undelivered = redis.call('XLEN', KEYS[3]) - pending
local fed = 0
while undelivered < tonumber(ARGV[5]) do
  local key = redis.call('LPOP', KEYS[1])
  if not key then break end
  local data = redis.call('LPOP', ARGV[2] .. key)
  if data then
    redis.call('XADD', KEYS[3], '*', 'data', data, 'task', ARGV[4])
    undelivered = undelivered + 1
    fed = fed + 1
  end
  if redis.call('LLEN', ARGV[2] .. key) > 0 then
    redis.call('RPUSH', KEYS[1], key)
  else
    redis.call('SREM', KEYS[2], key)
  end
end
return fed
""",
)

# Debounce: replace a key's waiting message with a newer one and push its
# time back.
# KEYS: 1 delayed zset, 2 debounce key (per task and key), 3 queue index,
#       4 queue streams set, 5 target stream
# ARGV: 2 new delayed member, 3 delay ms, 4 task id, 5 queue, 6 group
# Returns {'new'} or {'replaced', old task id}.
DEBOUNCE = Script(
    "queue_debounce",
    _HELPERS
    + """
local status = {'new'}
local old = redis.call('GET', KEYS[2])
if old then
  local sep = string.find(old, '|', 1, true)
  local old_task, old_member = string.sub(old, 1, sep - 1), string.sub(old, sep + 1)
  if redis.call('ZREM', KEYS[1], old_member) == 1 then status = {'replaced', old_task} end
end
redis.call('SADD', KEYS[3], ARGV[5])
if redis.call('SADD', KEYS[4], KEYS[5]) == 1 or redis.call('EXISTS', KEYS[5]) == 0 then
  ensure_group(KEYS[5], ARGV[6])
end
redis.call('ZADD', KEYS[1], now_ms() + tonumber(ARGV[3]), ARGV[2])
redis.call('SET', KEYS[2], ARGV[4] .. '|' .. ARGV[2], 'PX', tonumber(ARGV[3]) + 60000)
return status
""",
)

# Per-key rate limit of a taken message: run it now, or put it back as a
# delayed message for when its key's rate allows (other keys don't wait).
# KEYS: 1 rate key (per task and key), 2 delayed zset, 3 stream
# ARGV: 2 emission ms, 3 burst, 4 group, 5 entry id, 6 delayed member
# Returns {'ok'} or {'deferred', wait ms}.
RATE_DEFER = Script(
    "queue_rate_defer",
    """
local now = now_ms()
local emission = tonumber(ARGV[2])
local tolerance = emission * tonumber(ARGV[3])
local tat = tonumber(redis.call('GET', KEYS[1]) or now)
if tat < now then tat = now end
local new_tat = tat + emission
local allow_at = new_tat - tolerance
if allow_at <= now then
  redis.call('SET', KEYS[1], tostring(new_tat), 'PX', math.ceil(new_tat - now) + 1000)
  return {'ok'}
end
-- Not charged now: the message is checked again when it comes back.
local wait = math.ceil(allow_at - now)
redis.call('ZADD', KEYS[2], now + wait, ARGV[6])
redis.call('XACK', KEYS[3], ARGV[4], ARGV[5])
redis.call('XDEL', KEYS[3], ARGV[5])
return {'deferred', tostring(wait)}
""",
)

_DEAD_LETTER = """
local function dead_letter(dlq, max, stream, id, fields, reason, error, queue, now)
  redis.call('XADD', dlq, 'MAXLEN', '~', max, '*',
    'data', field(fields, 'data') or '',
    'task', field(fields, 'task') or '',
    'reason', reason, 'error', error, 'stream', stream,
    'queue', queue, 'at', tostring(now))
end
"""

# Take over entries other consumers left idle; dead-letter the ones
# delivered too often.
# KEYS: 1 stream, 2 dead-letter stream
# ARGV: 2 group, 3 consumer, 4 min idle ms, 5 count, 6 max deliveries,
#       7 queue, 8 dead-letter max
# Returns flat {id, fields, id, fields, ...}.
CLAIM_ABANDONED = Script(
    "queue_claim",
    _HELPERS
    + _DEAD_LETTER
    + """
local ok, reply = pcall(redis.call, 'XAUTOCLAIM', KEYS[1], ARGV[2], ARGV[3], ARGV[4], '0-0', 'COUNT', ARGV[5])
if not ok then return {} end
local out = {}
local now = now_ms()
for _, entry in ipairs(reply[2]) do
  local id, fields = entry[1], entry[2]
  if fields then
    local p = redis.call('XPENDING', KEYS[1], ARGV[2], id, id, 1)
    local count = 0
    if p[1] then count = tonumber(p[1][4]) end
    if count > tonumber(ARGV[6]) then
      dead_letter(KEYS[2], ARGV[8], KEYS[1], id, fields, 'max_deliveries',
        'delivered ' .. count .. ' times without being acknowledged', ARGV[7], now)
      redis.call('XACK', KEYS[1], ARGV[2], id)
      redis.call('XDEL', KEYS[1], id)
    else
      table.insert(out, id)
      table.insert(out, fields)
    end
  end
end
return out
""",
)

# Take one message from a dedicated (gated) task stream, but only if a
# cluster slot and the rate limit allow it. Slot and message are taken
# together or not at all.
# KEYS: 1 stream, 2 semaphore zset, 3 semaphore index, 4 rate key, 5 dead letters
# ARGV: 2 group, 3 consumer, 4 holder, 5 limit|'', 6 slot ttl ms,
#       7 emission ms|'', 8 burst, 9 claim idle ms|'' (skip claim),
#       10 max deliveries, 11 semaphore name, 12 queue, 13 dead-letter max
# Returns {'ok', id, fields} | {'busy'} | {'rate', wait ms} | {'empty'}.
GATED_TAKE = Script(
    "queue_gated_take",
    _HELPERS
    + _DEAD_LETTER
    + """
local now = now_ms()
if ARGV[5] ~= '' then
  redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
  if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[5]) then return {'busy'} end
end
local new_tat = nil
if ARGV[7] ~= '' then
  local emission = tonumber(ARGV[7])
  local tolerance = emission * tonumber(ARGV[8])
  local tat = tonumber(redis.call('GET', KEYS[4]) or now)
  if tat < now then tat = now end
  new_tat = tat + emission
  local allow_at = new_tat - tolerance
  if allow_at > now then return {'rate', tostring(math.ceil(allow_at - now))} end
end
local id, fields = nil, nil
if ARGV[9] ~= '' then
  local ok, reply = pcall(redis.call, 'XAUTOCLAIM', KEYS[1], ARGV[2], ARGV[3], ARGV[9], '0-0', 'COUNT', 1)
  if ok then
    for _, entry in ipairs(reply[2]) do
      if entry[2] then
        local p = redis.call('XPENDING', KEYS[1], ARGV[2], entry[1], entry[1], 1)
        local count = 0
        if p[1] then count = tonumber(p[1][4]) end
        if count > tonumber(ARGV[10]) then
          dead_letter(KEYS[5], ARGV[13], KEYS[1], entry[1], entry[2], 'max_deliveries',
            'delivered ' .. count .. ' times without being acknowledged', ARGV[12], now)
          redis.call('XACK', KEYS[1], ARGV[2], entry[1])
          redis.call('XDEL', KEYS[1], entry[1])
        else
          id, fields = entry[1], entry[2]
        end
      end
    end
  end
end
if not id then
  local ok, reply = pcall(redis.call, 'XREADGROUP', 'GROUP', ARGV[2], ARGV[3], 'COUNT', 1, 'STREAMS', KEYS[1], '>')
  if not ok then
    ensure_group(KEYS[1], ARGV[2])
    return {'empty'}
  end
  if reply and reply[1] and reply[1][2][1] then
    id, fields = reply[1][2][1][1], reply[1][2][1][2]
  end
end
if not id then return {'empty'} end
if ARGV[5] ~= '' then
  redis.call('ZADD', KEYS[2], now + tonumber(ARGV[6]), ARGV[4])
  redis.call('SADD', KEYS[3], ARGV[11])
end
if new_tat then
  redis.call('SET', KEYS[4], tostring(new_tat), 'PX', math.ceil(new_tat - now) + 1000)
end
return {'ok', id, fields}
""",
)

# Acknowledge and delete a processed entry.
# KEYS: 1 stream; ARGV: 2 group, 3 id
ACK = Script(
    "queue_ack",
    """
redis.call('XACK', KEYS[1], ARGV[2], ARGV[3])
return redis.call('XDEL', KEYS[1], ARGV[3])
""",
)

# Move an entry to the dead letters.
# KEYS: 1 stream, 2 dead letters
# ARGV: 2 group, 3 id, 4 data, 5 task, 6 reason, 7 error, 8 queue, 9 dlq max
DEAD_LETTER = Script(
    "queue_dead_letter",
    """
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[9], '*',
  'data', ARGV[4], 'task', ARGV[5], 'reason', ARGV[6], 'error', ARGV[7],
  'stream', KEYS[1], 'queue', ARGV[8], 'at', tostring(now_ms()))
if ARGV[3] ~= '' then
  redis.call('XACK', KEYS[1], ARGV[2], ARGV[3])
  redis.call('XDEL', KEYS[1], ARGV[3])
end
return 1
""",
)

# Put a taken-but-unprocessed entry back at the tail (e.g. on shutdown).
# KEYS: 1 stream, 2 wake list; ARGV: 2 group, 3 id, 4 data, 5 task, 6 wake '1'|''
REQUEUE = Script(
    "queue_requeue",
    _HELPERS
    + """
redis.call('XADD', KEYS[1], '*', 'data', ARGV[4], 'task', ARGV[5])
redis.call('XACK', KEYS[1], ARGV[2], ARGV[3])
redis.call('XDEL', KEYS[1], ARGV[3])
if ARGV[6] == '1' then wake(KEYS[2]) end
return 1
""",
)

# Per-key concurrency. Take a key slot for an entry, or park the entry.
# KEYS: 1 key semaphore zset, 2 park list, 3 stream, 4 semaphore index, 5 parks index
# ARGV: 2 holder, 3 limit, 4 ttl ms, 5 group, 6 id, 7 data, 8 task,
#       9 transferred slot holder|'', 10 semaphore name, 11 mode ('park'|'drop')
# Returns 1 (run it) or 0 (parked, or dropped in 'drop' mode).
KEY_ACQUIRE = Script(
    "queue_key_acquire",
    """
local now = now_ms()
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if ARGV[9] ~= '' and redis.call('ZREM', KEYS[1], ARGV[9]) == 1 then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), ARGV[2])
  return 1
end
if redis.call('LLEN', KEYS[2]) == 0 and redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), ARGV[2])
  redis.call('SADD', KEYS[4], ARGV[10])
  return 1
end
if ARGV[11] == 'drop' then
  redis.call('XACK', KEYS[3], ARGV[5], ARGV[6])
  redis.call('XDEL', KEYS[3], ARGV[6])
  return 0
end
redis.call('RPUSH', KEYS[2], string.format('%04d', #KEYS[3]) .. KEYS[3]
  .. string.format('%04d', #ARGV[8]) .. ARGV[8] .. ARGV[7])
redis.call('SADD', KEYS[5], KEYS[2] .. '|' .. KEYS[1] .. '|' .. ARGV[3])
redis.call('XACK', KEYS[3], ARGV[5], ARGV[6])
redis.call('XDEL', KEYS[3], ARGV[6])
return 0
""",
)

_UNPARK = """
local function unpark(sem, park, ttl, group, prefix)
  local item = redis.call('LPOP', park)
  if not item then return 0 end
  local slen = tonumber(string.sub(item, 1, 4))
  local stream = string.sub(item, 5, 4 + slen)
  local pos = 5 + slen
  local tlen = tonumber(string.sub(item, pos, pos + 3))
  local task = string.sub(item, pos + 4, pos + 3 + tlen)
  local data = string.sub(item, pos + 4 + tlen)
  local slot = 'parked|' .. redis.call('INCR', prefix .. 'lim:parked')
  redis.call('ZADD', sem, now_ms() + tonumber(ttl), slot)
  ensure_group(stream, group)
  redis.call('XADD', stream, '*', 'data', data, 'task', task, 'slot', slot)
  if string.find(stream, ':t:', 1, true) then
    wake(prefix .. 'lim:wake:task:' .. task)
  end
  return 1
end
"""

# Release a key slot; hand it straight to the next parked entry (keeps order).
# KEYS: 1 key semaphore zset, 2 park list
# ARGV: 2 holder, 3 transfer ttl ms, 4 group, 5 key prefix
KEY_RELEASE = Script(
    "queue_key_release",
    _HELPERS
    + _UNPARK
    + """
redis.call('ZREM', KEYS[1], ARGV[2])
return unpark(KEYS[1], KEYS[2], ARGV[3], ARGV[4], ARGV[5])
""",
)

# Janitor: unpark entries whose key has free slots (holders expired).
# KEYS: 1 parks index; ARGV: 2 transfer ttl ms, 3 group, 4 key prefix
UNPARK_FREE = Script(
    "queue_unpark_free",
    _HELPERS
    + _UNPARK
    + """
local now = now_ms()
local moved = 0
for _, entry in ipairs(redis.call('SMEMBERS', KEYS[1])) do
  local a = string.find(entry, '|', 1, true)
  local b = string.find(entry, '|', a + 1, true)
  local park = string.sub(entry, 1, a - 1)
  local sem = string.sub(entry, a + 1, b - 1)
  local limit = tonumber(string.sub(entry, b + 1))
  if redis.call('LLEN', park) == 0 then
    redis.call('SREM', KEYS[1], entry)
  else
    redis.call('ZREMRANGEBYSCORE', sem, '-inf', now)
    while redis.call('ZCARD', sem) < limit and redis.call('LLEN', park) > 0 do
      moved = moved + unpark(sem, park, ARGV[2], ARGV[3], ARGV[4])
    end
  end
end
return moved
""",
)
