"""
Lua scripts of the scheduler.

Keys (under ``syntasks:{<app>}:sched:``):

=====================  ========================================================
``def:<id>``           HASH: spec JSON + runtime state
``due:<shard>``        ZSET: id → next dispatch time (active schedules only)
``expiry``             ZSET: id → expiry time (dynamic schedules with a TTL)
``seq``                STRING: creation counter (index scores, in creation order)
``ids``                ZSET: id → creation number (listing)
``dynamic``            SET: ids of runtime schedules (quota)
``by-task:<task>``     ZSET: id → creation number, per task (template)
``owner:<owner>``      ZSET: id → creation number, per owner (runtime schedules)
``tag:<tag>``          ZSET: id → creation number, per tag (runtime schedules)
``templates``          SET: tasks that have runtime schedules
``finished``           ZSET: id → delete time, finished runtime schedules kept a while
``static``             HASH: id → fingerprint of declared schedules
``static:meta``        HASH: revision, fingerprint, synced_at, synced_by
``runs:<id>``          LIST: recent runs (JSON), newest first
=====================  ========================================================

No key here has a Redis TTL; expiry of runtime schedules is logical.
"""

from syntasks.cluster.scripts import Script
from syntasks.queues.scripts import _HELPERS

# Helpers shared by the scripts below. ``base`` is the ``sched:`` prefix,
# derived from a def key (``<base>def:<id>``).
_SCHED = """
local function sched_base(def, id) return string.sub(def, 1, #def - #id - 4) end
local function mark_done(def, id)
  local at = now_ms()
  redis.call('HSET', def, 'state', 'done', 'finished_at', tostring(at))
  redis.call('HDEL', def, 'next', 'nominal')
  local keep = redis.call('HGET', def, 'keep_ms')
  if keep then
    redis.call('ZADD', sched_base(def, id) .. 'finished', at + tonumber(keep), id)
  end
end
local function unindex(def, id)
  local base = sched_base(def, id)
  local owner = redis.call('HGET', def, 'owner')
  if owner and owner ~= '' then redis.call('ZREM', base .. 'owner:' .. owner, id) end
  local tags = redis.call('HGET', def, 'tags')
  if tags and tags ~= '' then
    for _, tag in ipairs(cjson.decode(tags)) do redis.call('ZREM', base .. 'tag:' .. tag, id) end
  end
end
"""

# Create or replace a schedule, keeping every index consistent.
# KEYS: 1 def, 2 due, 3 expiry, 4 ids, 5 dynamic, 6 by-task (new task)
# ARGV: 2 id, 3 spec json, 4 source, 5 task, 6 next due|'', 7 nominal|'',
#       8 expires at|'', 9 if_exists (error|replace|ignore), 10 max dynamic|'',
#       11 state, 12 expected version|'', 13 keep runtime ('1'|''),
#       14 by-task key prefix, 15 max_runs|'', 16 owner|'', 17 tags json|'',
#       18 meta json|'', 19 max per owner|'', 20 keep finished ms|''
UPSERT = Script(
    "sched_upsert",
    _SCHED
    + """
local now = now_ms()
local exists = redis.call('EXISTS', KEYS[1]) == 1
local base = sched_base(KEYS[1], ARGV[2])
if ARGV[16] ~= '' and ARGV[19] ~= '' then
  local owner_key = base .. 'owner:' .. ARGV[16]
  local count = redis.call('ZCARD', owner_key)
  if not redis.call('ZSCORE', owner_key, ARGV[2]) and count >= tonumber(ARGV[19]) then
    return {'owner_quota', tostring(count)}
  end
end
if exists then
  if ARGV[9] == 'error' then return {'exists'} end
  if ARGV[9] == 'ignore' then return {'ignored'} end
  local source = redis.call('HGET', KEYS[1], 'source')
  if ARGV[4] == 'api' and source ~= 'api' then return {'static'} end
  if ARGV[12] ~= '' and redis.call('HGET', KEYS[1], 'version') ~= ARGV[12] then
    return {'conflict', redis.call('HGET', KEYS[1], 'version')}
  end
  local old_task = redis.call('HGET', KEYS[1], 'task')
  if old_task and old_task ~= ARGV[5] then
    redis.call('ZREM', ARGV[14] .. old_task, ARGV[2])
  end
  unindex(KEYS[1], ARGV[2])
else
  if ARGV[12] ~= '' then return {'missing'} end
  if ARGV[4] == 'api' and ARGV[10] ~= '' and redis.call('SCARD', KEYS[5]) >= tonumber(ARGV[10]) then
    return {'quota'}
  end
  local seq = redis.call('INCR', base .. 'seq')
  redis.call('HSET', KEYS[1], 'created_at', tostring(now), 'seq', seq, 'runs', '0',
    'failures', '0')
  redis.call('HSET', KEYS[1], 'state', ARGV[11])
  redis.call('ZADD', KEYS[4], seq, ARGV[2])
end
redis.call('HSET', KEYS[1], 'spec', ARGV[3], 'source', ARGV[4], 'task', ARGV[5],
  'updated_at', tostring(now))
redis.call('HINCRBY', KEYS[1], 'version', 1)
if ARGV[15] ~= '' then redis.call('HSET', KEYS[1], 'max_runs', ARGV[15])
else redis.call('HDEL', KEYS[1], 'max_runs') end
if ARGV[4] == 'api' then
  redis.call('SADD', KEYS[5], ARGV[2])
  redis.call('SADD', base .. 'templates', ARGV[5])
else
  redis.call('SREM', KEYS[5], ARGV[2])
end
local created = redis.call('HGET', KEYS[1], 'seq')
redis.call('ZADD', KEYS[6], created, ARGV[2])
redis.call('HSET', KEYS[1], 'owner', ARGV[16], 'tags', ARGV[17], 'meta', ARGV[18])
if ARGV[16] ~= '' then redis.call('ZADD', base .. 'owner:' .. ARGV[16], created, ARGV[2]) end
if ARGV[17] ~= '' then
  for _, tag in ipairs(cjson.decode(ARGV[17])) do
    redis.call('ZADD', base .. 'tag:' .. tag, created, ARGV[2])
  end
end
if ARGV[20] ~= '' then redis.call('HSET', KEYS[1], 'keep_ms', ARGV[20])
else redis.call('HDEL', KEYS[1], 'keep_ms') end
if ARGV[8] ~= '' then
  redis.call('HSET', KEYS[1], 'expires_at', ARGV[8])
  redis.call('ZADD', KEYS[3], ARGV[8], ARGV[2])
else
  redis.call('HDEL', KEYS[1], 'expires_at')
  redis.call('ZREM', KEYS[3], ARGV[2])
end
if ARGV[13] ~= '1' or not exists then
  redis.call('HDEL', KEYS[1], 'claim_token', 'claim_fire', 'invalid_reason')
  if ARGV[6] == '' then
    mark_done(KEYS[1], ARGV[2])
    redis.call('ZREM', KEYS[2], ARGV[2])
  else
    redis.call('ZREM', base .. 'finished', ARGV[2])
    redis.call('HDEL', KEYS[1], 'finished_at')
    redis.call('HSET', KEYS[1], 'next', ARGV[6], 'nominal', ARGV[7])
    local state = redis.call('HGET', KEYS[1], 'state')
    if state == 'done' or state == 'invalid' or state == 'orphaned' then
      redis.call('HSET', KEYS[1], 'state', 'active')
      state = 'active'
    end
    if exists and ARGV[13] ~= '1' and ARGV[11] == 'paused' then
      redis.call('HSET', KEYS[1], 'state', 'paused')
      state = 'paused'
    end
    if state == 'active' then
      redis.call('ZADD', KEYS[2], ARGV[6], ARGV[2])
    else
      redis.call('ZREM', KEYS[2], ARGV[2])
    end
  end
end
return {'ok', redis.call('HGET', KEYS[1], 'version')}
""",
)

# Delete a schedule and its indexes.
# KEYS: 1 def, 2 due, 3 expiry, 4 ids, 5 dynamic, 6 runs, 7 static
# ARGV: 2 id, 3 by-task key prefix, 4 allow static ('1'|'')
DELETE = Script(
    "sched_delete",
    _SCHED
    + """
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local source = redis.call('HGET', KEYS[1], 'source')
if source ~= 'api' and ARGV[4] ~= '1' then return -1 end
local task = redis.call('HGET', KEYS[1], 'task')
if task then redis.call('ZREM', ARGV[3] .. task, ARGV[2]) end
unindex(KEYS[1], ARGV[2])
redis.call('ZREM', sched_base(KEYS[1], ARGV[2]) .. 'finished', ARGV[2])
redis.call('DEL', KEYS[1], KEYS[6])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('ZREM', KEYS[4], ARGV[2])
redis.call('SREM', KEYS[5], ARGV[2])
redis.call('HDEL', KEYS[7], ARGV[2])
return 1
""",
)

# Pause or resume.
# KEYS: 1 def, 2 due; ARGV: 2 id, 3 'pause'|'resume', 4 next due|'', 5 nominal|''
SET_STATE = Script(
    "sched_set_state",
    _SCHED
    + """
if redis.call('EXISTS', KEYS[1]) == 0 then return {'missing'} end
local state = redis.call('HGET', KEYS[1], 'state')
if ARGV[3] == 'pause' then
  if state == 'active' then
    redis.call('HSET', KEYS[1], 'state', 'paused')
    redis.call('ZREM', KEYS[2], ARGV[2])
  end
else
  if state == 'paused' or state == 'active' then
    if ARGV[4] == '' then
      mark_done(KEYS[1], ARGV[2])
      redis.call('ZREM', KEYS[2], ARGV[2])
    else
      redis.call('HSET', KEYS[1], 'state', 'active', 'next', ARGV[4], 'nominal', ARGV[5],
        'failures', '0')
      redis.call('HDEL', KEYS[1], 'claim_token', 'claim_fire')
      redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2])
    end
  end
end
redis.call('HINCRBY', KEYS[1], 'version', 1)
redis.call('HSET', KEYS[1], 'updated_at', tostring(now_ms()))
return {'ok', redis.call('HGET', KEYS[1], 'state')}
""",
)

# Claim due schedules for dispatching.
# KEYS: 1 due
# ARGV: 2 batch, 3 claim lease ms, 4 def key prefix, 5 token
# Returns flat {id, fire, spec, runs, ...}.
CLAIM = Script(
    "sched_claim",
    _SCHED
    + """
local now = now_ms()
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
local out = {}
for _, id in ipairs(ids) do
  local def = ARGV[4] .. id
  local data = redis.call('HMGET', def, 'state', 'expires_at', 'runs', 'max_runs',
    'nominal', 'claim_fire', 'spec')
  local state, expires, runs, max_runs, nominal, claimed, spec =
    data[1], data[2], data[3], data[4], data[5], data[6], data[7]
  if not state or not spec then
    redis.call('ZREM', KEYS[1], id)
  elseif state ~= 'active' then
    redis.call('ZREM', KEYS[1], id)
  elseif expires and tonumber(expires) <= now then
    redis.call('ZREM', KEYS[1], id)
    redis.call('HSET', def, 'state', 'expired')
  elseif max_runs and tonumber(runs or '0') >= tonumber(max_runs) then
    redis.call('ZREM', KEYS[1], id)
    mark_done(def, id)
  else
    local fire = claimed or nominal
    if not fire then fire = tostring(now) end
    redis.call('HSET', def, 'claim_token', ARGV[5], 'claim_fire', fire)
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), id)
    table.insert(out, id)
    table.insert(out, fire)
    table.insert(out, spec)
    table.insert(out, runs or '0')
  end
end
return out
""",
)

# Enqueue a schedule's fires and move it to its next fire, atomically.
# KEYS: 1 def, 2 due, 3 runs list, 4 stream, 5 queue streams set,
#       6 queue index, 7 wake list, 8 slot zset (overlap)
# ARGV: 2 token, 3 next due|'', 4 nominal|'', 5 group, 6 task,
#       7 queue, 8 overlap, 9 max instances, 10 history, 11 wake '1'|'',
#       12 last fire, 13 skipped (missed) count, 14 id,
#       15.. triples: data, fire, task id
COMMIT = Script(
    "sched_commit",
    _HELPERS
    + _SCHED
    + """
if redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[2] then return {'lost'} end
local now = now_ms()
local running = 0
if ARGV[8] == 'skip' then
  redis.call('ZREMRANGEBYSCORE', KEYS[8], '-inf', now)
  running = redis.call('ZCARD', KEYS[8])
end
local fired, skipped = 0, tonumber(ARGV[13])
local last_task = nil
local i = 15
while ARGV[i] do
  local data, fire, task_id = ARGV[i], ARGV[i + 1], ARGV[i + 2]
  if ARGV[8] == 'skip' and running + fired >= tonumber(ARGV[9]) then
    skipped = skipped + 1
    redis.call('LPUSH', KEYS[3], cjson.encode({fire = tonumber(fire), task_id = task_id,
      status = 'skipped', at = now}))
  else
    redis.call('SADD', KEYS[6], ARGV[7])
    if redis.call('SADD', KEYS[5], KEYS[4]) == 1 or redis.call('EXISTS', KEYS[4]) == 0 then
      ensure_group(KEYS[4], ARGV[5])
    end
    redis.call('XADD', KEYS[4], '*', 'data', data, 'task', ARGV[6])
    if ARGV[11] == '1' then wake(KEYS[7]) end
    fired = fired + 1
    last_task = task_id
  end
  i = i + 3
end
redis.call('LTRIM', KEYS[3], 0, tonumber(ARGV[10]) - 1)
redis.call('HDEL', KEYS[1], 'claim_token', 'claim_fire')
redis.call('HINCRBY', KEYS[1], 'runs', fired)
if skipped > 0 then redis.call('HINCRBY', KEYS[1], 'skipped', skipped) end
redis.call('HSET', KEYS[1], 'last_fire', ARGV[12])
if last_task then redis.call('HSET', KEYS[1], 'last_task_id', last_task) end
local state = redis.call('HGET', KEYS[1], 'state')
if ARGV[3] == '' then
  redis.call('ZREM', KEYS[2], ARGV[14])
  mark_done(KEYS[1], ARGV[14])
else
  redis.call('HSET', KEYS[1], 'next', ARGV[3], 'nominal', ARGV[4])
  if state == 'active' then
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[14])
  else
    redis.call('ZREM', KEYS[2], ARGV[14])
  end
end
return {'ok', tostring(fired), tostring(skipped)}
""",
)

# Record the end of a scheduled run.
# KEYS: 1 def, 2 due, 3 runs list
# ARGV: 2 status, 3 error, 4 entry json, 5 history, 6 pause after failures|'', 7 id
RECORD = Script(
    "sched_record",
    """
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'last_status', ARGV[2], 'last_error', ARGV[3])
local failures = 0
if ARGV[2] == 'ok' then
  redis.call('HSET', KEYS[1], 'failures', '0')
else
  failures = redis.call('HINCRBY', KEYS[1], 'failures', 1)
end
redis.call('LPUSH', KEYS[3], ARGV[4])
redis.call('LTRIM', KEYS[3], 0, tonumber(ARGV[5]) - 1)
if ARGV[6] ~= '' and failures >= tonumber(ARGV[6])
   and redis.call('HGET', KEYS[1], 'state') == 'active' then
  redis.call('HSET', KEYS[1], 'state', 'paused')
  redis.call('ZREM', KEYS[2], ARGV[7])
  return 2
end
return 1
""",
)

# Begin syncing declared schedules.
# KEYS: 1 static meta, 2 sync lock
# ARGV: 2 revision|'', 3 set fingerprint, 4 node
SYNC_BEGIN = Script(
    "sched_sync_begin",
    """
local stored_rev = redis.call('HGET', KEYS[1], 'revision')
if stored_rev and stored_rev ~= '' and (ARGV[2] == '' or tonumber(stored_rev) > tonumber(ARGV[2])) then
  return {'stale', stored_rev}
end
if redis.call('HGET', KEYS[1], 'fingerprint') == ARGV[3] and (stored_rev or '') == ARGV[2] then
  return {'same', ''}
end
if not redis.call('SET', KEYS[2], ARGV[4], 'NX', 'PX', 30000) then
  return {'busy', ''}
end
return {'go', ''}
""",
)

# Finish syncing.
# KEYS: 1 static meta, 2 sync lock; ARGV: 2 revision|'', 3 fingerprint, 4 node
SYNC_END = Script(
    "sched_sync_end",
    """
redis.call('HSET', KEYS[1], 'revision', ARGV[2], 'fingerprint', ARGV[3],
  'synced_by', ARGV[4], 'synced_at', tostring(now_ms()))
if redis.call('GET', KEYS[2]) == ARGV[4] then redis.call('DEL', KEYS[2]) end
return 1
""",
)

# Mark a schedule invalid (e.g. its arguments don't fit the task).
# KEYS: 1 def, 2 due; ARGV: 2 id, 3 reason
INVALIDATE = Script(
    "sched_invalidate",
    """
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'state', 'invalid', 'invalid_reason', ARGV[3])
redis.call('HDEL', KEYS[1], 'claim_token', 'claim_fire')
redis.call('ZREM', KEYS[2], ARGV[2])
return 1
""",
)


# Orphan the runtime schedules of a template that left the code, or adopt
# them again when it is back.
# KEYS: 1 def, 2 due; ARGV: 2 id, 3 'orphan'|'adopt', 4 next due|'', 5 nominal|''
ORPHAN = Script(
    "sched_orphan",
    _SCHED
    + """
local state = redis.call('HGET', KEYS[1], 'state')
if not state then return 'missing' end
if ARGV[3] == 'orphan' then
  if state ~= 'active' and state ~= 'paused' then return state end
  redis.call('HSET', KEYS[1], 'state', 'orphaned', 'orphaned_from', state)
  redis.call('ZREM', KEYS[2], ARGV[2])
  return 'orphaned'
end
if state ~= 'orphaned' then return state end
local previous = redis.call('HGET', KEYS[1], 'orphaned_from') or 'active'
redis.call('HDEL', KEYS[1], 'orphaned_from')
if ARGV[4] == '' then
  mark_done(KEYS[1], ARGV[2])
  return 'done'
end
redis.call('HSET', KEYS[1], 'state', previous, 'next', ARGV[4], 'nominal', ARGV[5])
if previous == 'active' then redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2]) end
return previous
""",
)
