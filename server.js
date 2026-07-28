import express from 'express'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import QRCode from 'qrcode'
import { ConsoleLogger, createStore, WaClient } from 'zapo-js'
import { createSqliteStore } from '@zapo-js/store-sqlite'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const PORT = process.env.PORT || 3000

// ---------------------------------------------------------------------------
// Session state exposed to the UI
// ---------------------------------------------------------------------------
const state = {
  status: 'starting', // starting | waiting_qr | connecting | connected | disconnected | logged_out
  qrDataUrl: null,    // QR rendered as a data URL for the UI
  me: null,           // paired account jid
  lastError: null
}

let client = null
let groupsCache = []          // last known groups of the session
let groupsCacheAt = 0
let lastSendReport = null     // ack report of the most recent send (fast mode confirms via this)
const GROUPS_TTL_MS = 60_000  // refresh group list at most once per minute

// ---------------------------------------------------------------------------
// zapo-js client
// ---------------------------------------------------------------------------
function buildClient () {
  fs.mkdirSync(path.join(__dirname, '.auth'), { recursive: true })
  const store = createStore({
    backends: {
      sqlite: createSqliteStore({ path: '.auth/state.sqlite' })
    },
    providers: {
      auth: 'sqlite',
      signal: 'sqlite',
      preKey: 'sqlite',
      session: 'sqlite',
      identity: 'sqlite',
      senderKey: 'sqlite',
      appState: 'sqlite',
      privacyToken: 'sqlite',
      messages: 'none',
      threads: 'none',
      contacts: 'none'
    }
  })

  const c = new WaClient({ store, sessionId: 'default' }, new ConsoleLogger('info'))

  c.on('auth_qr', async ({ qr }) => {
    state.status = 'waiting_qr'
    state.qrDataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 320 })
    console.log('[zapo] new QR issued — scan it from the UI')
  })

  c.on('auth_paired', ({ credentials }) => {
    state.me = credentials.meJid
    state.qrDataUrl = null
    console.log('[zapo] paired as', credentials.meJid)
  })

  c.on('connection', async (event) => {
    if (event.status === 'open') {
      state.status = 'connected'
      state.qrDataUrl = null
      state.lastError = null
      const creds = c.getCredentials()
      if (creds) state.me = creds.meJid
      console.log('[zapo] connection open')
      refreshGroups(true).catch(() => {})
    } else {
      console.log('[zapo] connection closed:', event.reason, 'logout?', event.isLogout)
      if (event.isLogout) {
        state.status = 'logged_out'
        state.me = null
        groupsCache = []
      } else {
        state.status = 'disconnected'
        // zapo does not auto-reconnect by design — rebuild and reconnect
        setTimeout(() => startClient().catch(err => {
          state.lastError = String(err?.message || err)
        }), 3000)
      }
    }
  })

  return c
}

async function startClient () {
  state.status = 'connecting'
  client = buildClient()
  await client.connect()
}

// ---------------------------------------------------------------------------
// Groups available in the session
// ---------------------------------------------------------------------------
async function refreshGroups (force = false) {
  if (!client || state.status !== 'connected') return groupsCache
  if (!force && Date.now() - groupsCacheAt < GROUPS_TTL_MS) return groupsCache
  const groups = await client.group.queryAllGroups()
  groupsCache = groups.map(g => ({
    jid: g.jid,
    subject: g.subject,
    participants: g.participants?.length ?? 0,
    announce: !!g.announce
  }))
  groupsCacheAt = Date.now()
  console.log(`[zapo] session has ${groupsCache.length} groups`)
  return groupsCache
}

// ---------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------
const app = express()
app.use(express.json())
app.use(express.static(path.join(__dirname, 'public')))

// Connection / pairing status + QR for the UI to poll
app.get('/api/status', (req, res) => {
  res.json({
    status: state.status,
    qr: state.qrDataUrl,
    me: state.me,
    lastError: state.lastError
  })
})

// Groups the paired account belongs to (the only allowed recipients)
app.get('/api/groups', async (req, res) => {
  if (state.status !== 'connected') {
    return res.status(409).json({ error: 'Session is not connected yet' })
  }
  try {
    const groups = await refreshGroups(req.query.force === '1')
    res.json({ groups })
  } catch (err) {
    res.status(500).json({ error: String(err?.message || err) })
  }
})

// Send a text message — ONLY to groups that exist in the session (parallel fan-out)
app.post('/api/send', async (req, res) => {
  const { groupJids, groupJid, message, fast } = req.body || {}
  if (state.status !== 'connected') {
    return res.status(409).json({ error: 'Session is not connected yet' })
  }
  // Accept an array of JIDs, or a single one (backward compatible), de-duplicated
  const targets = Array.isArray(groupJids) ? groupJids : (groupJid ? [groupJid] : [])
  const jids = [...new Set(targets)]
  if (!jids.length) {
    return res.status(400).json({ error: 'Select at least one group' })
  }
  if (jids.some(j => typeof j !== 'string' || !j.endsWith('@g.us'))) {
    return res.status(400).json({ error: 'Every target must be a group JID ending in @g.us' })
  }
  if (typeof message !== 'string' || !message.trim()) {
    return res.status(400).json({ error: 'message is required' })
  }
  const text = message.trim()
  try {
    // Validate against the warm in-memory group set (refreshed on connect + via the UI).
    // Only touch the network if we have never loaded groups — never block a send otherwise.
    if (!groupsCache.length) await refreshGroups(true)
    const known = new Set(groupsCache.map(g => g.jid))
    const nameOf = new Map(groupsCache.map(g => [g.jid, g.subject]))

    // Fire every send at once — no delays, no batching, fully parallel over the socket.
    // linkPreview:false avoids a blocking URL-metadata fetch when a message contains a link.
    const startedAt = Date.now()
    const promises = jids.map(async (jid) => {
      if (!known.has(jid)) throw new Error('Not available in the current session')
      const t0 = Date.now()
      const result = await client.message.send(jid, { type: 'text', text, linkPreview: false })
      return { result, ms: Date.now() - t0 }
    })

    const finalize = (settled) => {
      const report = settled.map((r, i) => {
        const jid = jids[i]
        const subject = nameOf.get(jid) || jid
        return r.status === 'fulfilled'
          ? { jid, subject, ok: true, ms: r.value.ms }
          : { jid, subject, ok: false, error: String(r.reason?.message || r.reason) }
      })
      const sent = report.filter(r => r.ok).length
      lastSendReport = {
        at: Date.now(),
        sent,
        failed: report.length - sent,
        total: report.length,
        ms: Date.now() - startedAt,
        report
      }
      return lastSendReport
    }

    if (fast) {
      // ⚡ Fast mode: reply the instant the sends are dispatched — don't wait for server acks.
      lastSendReport = null
      res.json({ ok: true, fast: true, total: jids.length, ms: Date.now() - startedAt })
      Promise.allSettled(promises).then(finalize)
      return
    }

    const settled = await Promise.allSettled(promises)
    const { sent, failed, total, ms, report } = finalize(settled)
    res.json({ ok: sent > 0, sent, failed, total, ms, report })
  } catch (err) {
    res.status(500).json({ error: String(err?.message || err) })
  }
})

// Ack report of the most recent send — fast mode polls this to confirm delivery
app.get('/api/last-send', (req, res) => {
  res.json(lastSendReport || { pending: true })
})

// Unlink the device and clear the session
app.post('/api/logout', async (req, res) => {
  try {
    if (client) await client.logout()
    state.status = 'logged_out'
    state.me = null
    state.qrDataUrl = null
    groupsCache = []
    res.json({ ok: true })
    // start a fresh pairing cycle so a new QR appears
    setTimeout(() => startClient().catch(err => {
      state.lastError = String(err?.message || err)
    }), 1500)
  } catch (err) {
    res.status(500).json({ error: String(err?.message || err) })
  }
})

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`[http] UI ready on http://localhost:${PORT}`)
})

// Keep the group set warm in the background so sends never pay for a metadata lookup.
setInterval(() => { refreshGroups(true).catch(() => {}) }, 90_000)

startClient().catch(err => {
  state.status = 'disconnected'
  state.lastError = String(err?.message || err)
  console.error('[zapo] failed to connect:', err)
})

// Graceful shutdown — flush the write-behind store
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    try { if (client) await client.disconnect() } catch {}
    process.exit(0)
  })
}
