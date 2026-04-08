package redis

// Lua scripts. Keys are listed in the order Eval expects them.
//
// Score formula: -priority * 1e13 + run_at_ms. priority is in [-1, 1] so the
// per-priority "band" is 1e13 wide, far larger than any reasonable run_at in
// milliseconds (1e15 covers ~31k years), so within a band the smallest score
// is the soonest run_at.

const enqueueScript = `
-- KEYS[1] = queue:<t>:ready
-- KEYS[2] = queue:<t>:job:<id>
-- KEYS[3] = queue:<t>:notify
-- ARGV[1] = id
-- ARGV[2] = payload
-- ARGV[3] = priority
-- ARGV[4] = run_at_ms
-- ARGV[5] = enqueued_at_ms
local score = (-tonumber(ARGV[3])) * 1e13 + tonumber(ARGV[4])
redis.call('HSET', KEYS[2],
  'payload', ARGV[2],
  'priority', ARGV[3],
  'attempt', '0',
  'enqueued_at', ARGV[5])
redis.call('ZADD', KEYS[1], score, ARGV[1])
redis.call('RPUSH', KEYS[3], '1')
redis.call('LTRIM', KEYS[3], 0, 16)
return 1
`

// claimScript needs to know the eligible candidate id BEFORE it can construct
// the job hash key. To stay Redis-Cluster-friendly we use a hash tag in the
// key layout (queue:{<topic>}:ready, queue:{<topic>}:job:<id>) so all per-
// topic keys land in the same slot, then build the job key inside the script
// from a known prefix passed as ARGV[3].
//
// Score formula: (-priority) * 1e13 + run_at_ms. This orders by priority
// DESC then run_at ASC, but a single ZRANGEBYSCORE upper bound cannot
// express "ready now" because higher priority bands have lower scores even
// for far-future run_at values. Solution: loop over priority bands from
// highest to lowest and pick the first match within each band's "ready now"
// window.
const claimScript = `
-- KEYS[1] = queue:{<t>}:ready
-- KEYS[2] = queue:{<t>}:inflight
-- ARGV[1] = now_ms
-- ARGV[2] = visibility_ms
-- ARGV[3] = job_key_prefix (e.g. "queue:{<t>}:job:")
-- Returns: {id, payload, attempt, enqueued_at} or nil
local now = tonumber(ARGV[1])
local id = nil
-- Iterate priority bands high → low. Priority p has score band
-- [(-p)*1e13, (-p)*1e13 + 1e13). "Ready now" within the band means
-- run_at_ms <= now, i.e. score <= (-p)*1e13 + now.
for _, p in ipairs({1, 0, -1}) do
  local bandMin = (-p) * 1e13
  local bandCutoff = bandMin + now
  local ids = redis.call('ZRANGEBYSCORE', KEYS[1], bandMin, bandCutoff, 'LIMIT', 0, 1)
  if #ids > 0 then
    id = ids[1]
    break
  end
end
if id == nil then
  return nil
end
local jobKey = ARGV[3] .. id
redis.call('ZREM', KEYS[1], id)
local lockUntil = now + tonumber(ARGV[2])
redis.call('ZADD', KEYS[2], lockUntil, id)
redis.call('HINCRBY', jobKey, 'attempt', 1)
local payload = redis.call('HGET', jobKey, 'payload')
local attempt = redis.call('HGET', jobKey, 'attempt')
local enqueuedAt = redis.call('HGET', jobKey, 'enqueued_at')
return {id, payload, attempt, enqueuedAt}
`

const ackScript = `
-- KEYS[1] = queue:<t>:inflight
-- KEYS[2] = queue:<t>:job:<id>
-- ARGV[1] = id
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
`

const failScript = `
-- KEYS[1] = queue:<t>:inflight
-- KEYS[2] = queue:<t>:ready
-- KEYS[3] = queue:<t>:job:<id>
-- ARGV[1] = id
-- ARGV[2] = priority
-- ARGV[3] = new_run_at_ms
-- ARGV[4] = err
redis.call('ZREM', KEYS[1], ARGV[1])
local score = (-tonumber(ARGV[2])) * 1e13 + tonumber(ARGV[3])
redis.call('ZADD', KEYS[2], score, ARGV[1])
redis.call('HSET', KEYS[3], 'last_error', ARGV[4])
return 1
`

const reapScript = `
-- KEYS[1] = queue:<t>:inflight
-- KEYS[2] = queue:<t>:ready
-- ARGV[1] = now_ms
-- Move all expired inflight entries back to ready (preserve priority via
-- score lookup is tricky; we just use score = now so they go to the
-- highest-priority band's "now" position — they get retried promptly).
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local moved = 0
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('ZADD', KEYS[2], 1 * 1e13 + tonumber(ARGV[1]), id)
  moved = moved + 1
end
return moved
`
