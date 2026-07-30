import express from 'express'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import multer from 'multer'
import QRCode from 'qrcode'
import { fileTypeFromFile } from 'file-type'
import { ConsoleLogger, createStore, WaClient } from 'zapo-js'
import { createSqliteStore } from '@zapo-js/store-sqlite'
import { createMediaProcessor } from '@zapo-js/media-utils'
import ffmpegPath from 'ffmpeg-static'
import ffprobeStatic from 'ffprobe-static'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const PORT = process.env.PORT || 3500

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
let store = null              // sqlite-backed store of the current client (destroyed on rebuild/logout)
let resetting = null          // in-flight hard reset, deduped so logout + close-handler don't race
let groupsCache = []          // last known groups of the session
let groupsCacheAt = 0
let lastSendReport = null     // ack report of the most recent send (fast mode confirms via this)
let reconnectTimer = null     // dedupes reconnect scheduling (logout + closed-handler race)
let starting = false          // guards against two clients being built at once
const GROUPS_TTL_MS = 60_000  // refresh group list at most once per minute
const SEND_BATCH_SIZE = 5        // groups sent in parallel per batch
const SEND_BATCH_DELAY_MS = 100  // pause between batches (0.1s), applies to all modes

// Media processor — sharp for image thumbnails, vendored ffmpeg/ffprobe for video
// thumbnails + duration/dimension probing. Defaults match the official WhatsApp
// clients (JPEG thumb, WA-standard max edge). Shared across client rebuilds.
const mediaProcessor = createMediaProcessor({
  ffmpegPath,
  ffprobePath: ffprobeStatic.path
})

// ---------------------------------------------------------------------------
// zapo-js client
// ---------------------------------------------------------------------------
function buildClient () {
  fs.mkdirSync(path.join(__dirname, '.auth'), { recursive: true })
  store = createStore({
    backends: {
      sqlite: createSqliteStore({
        path: '.auth/state.sqlite',
        // keep device lists / group metadata warm for 1h — push events update them in between
        cacheTtlMs: { deviceListMs: 3_600_000, groupMetadataMs: 3_600_000 }
      })
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
    },
    // Persist the hot fan-out caches. Memory-only (the default) means every server
    // restart pays a usync round-trip per participant + a rate-limited metadata IQ
    // on the FIRST send — this is what makes the first send slow.
    cacheProviders: {
      deviceList: 'sqlite',
      groupMetadata: 'sqlite'
    },
    // In-process LRU in front of sqlite for the per-message crypto reads (safe:
    // this process is the only writer for this sessionId).
    cacheLayer: { session: true, identity: true, senderKey: true, privacyToken: true }
  })

  const c = new WaClient({
    store,
    sessionId: 'default',
    // auto-generate WA-standard thumbnails + probe duration/dimensions on outgoing media
    media: { processor: mediaProcessor, generateThumbnail: true, generateProbe: true }
  }, new ConsoleLogger('info'))

  c.on('auth_qr', async ({ qr }) => {
    if (c !== client) return // stale instance replaced by a newer one
    state.status = 'waiting_qr'
    state.qrDataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 320 })
    console.log('[zapo] new QR issued — scan it from the UI')
  })

  c.on('auth_paired', ({ credentials }) => {
    if (c !== client) return
    state.me = credentials.meJid
    state.qrDataUrl = null
    console.log('[zapo] paired as', credentials.meJid)
  })

  c.on('connection', async (event) => {
    if (c !== client) return // stale instance replaced by a newer one
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
        // device removed (UI logout or unlinked from the phone) — wipe the session
        // completely and restart pairing so a fresh QR appears
        hardResetSession().catch(() => {})
      } else {
        state.status = 'disconnected'
        // zapo does not auto-reconnect by design — rebuild and reconnect
        scheduleReconnect(3000)
      }
    }
  })

  return c
}

// Single entry point for reconnects — deduped so racing callers can't spawn two clients
function scheduleReconnect (delayMs) {
  if (reconnectTimer) return
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null
    startClient().catch(err => {
      state.status = 'disconnected'
      state.lastError = String(err?.message || err)
    })
  }, delayMs)
}

async function startClient () {
  if (starting) return
  starting = true
  try {
    state.status = 'connecting'
    const oldClient = client
    const oldStore = store
    client = null
    store = null
    if (oldClient) { try { await oldClient.disconnect() } catch {} } // make sure a stale socket is dead
    if (oldStore) { try { await oldStore.destroy() } catch {} }      // release the sqlite handle before reopening
    client = buildClient()
    await client.connect()
  } finally {
    starting = false
  }
}

// Delete the persisted session (.auth/state.sqlite + -wal/-shm). The store must be
// destroyed first or Windows keeps the file locked — hence the retries.
function wipeSessionFiles () {
  try {
    fs.rmSync(path.join(__dirname, '.auth'), { recursive: true, force: true, maxRetries: 10, retryDelay: 200 })
    console.log('[zapo] session files deleted')
  } catch (err) {
    console.warn('[zapo] could not delete session files:', String(err?.message || err))
  }
}

// Full teardown: disconnect the client, close the sqlite store, delete the session
// files, then restart pairing. Deduped — the logout route and the isLogout close
// handler can both call this without racing each other.
function hardResetSession () {
  if (resetting) return resetting
  resetting = (async () => {
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null }
    const c = client
    const s = store
    client = null // detach first so stale event handlers become no-ops
    store = null
    if (c) { try { await c.disconnect() } catch {} }
    if (s) { try { await s.destroy() } catch {} }
    wipeSessionFiles()
    state.status = 'logged_out'
    state.me = null
    state.qrDataUrl = null
    state.lastError = null
    groupsCache = []
    lastSendReport = null
    scheduleReconnect(1000) // fresh pairing cycle → new QR
  })().finally(() => { resetting = null })
  return resetting
}

// ---------------------------------------------------------------------------
// Groups available in the session
// ---------------------------------------------------------------------------
async function refreshGroups (force = false) {
  if (!client || state.status !== 'connected') return groupsCache
  if (!force && Date.now() - groupsCacheAt < GROUPS_TTL_MS) return groupsCache
  try {
    const groups = await client.group.queryAllGroups()
    groupsCache = groups.map(g => ({
      jid: g.jid,
      subject: g.subject,
      participants: g.participants?.length ?? 0,
      announce: !!g.announce
    }))
    groupsCacheAt = Date.now()
    console.log(`[zapo] session has ${groupsCache.length} groups`)
  } catch (err) {
    // Transient socket death (keepalive resume, reconnect in flight) kills in-flight
    // queries — serve the last known list instead of erroring; the connection-open
    // handler force-refreshes right after the reconnect lands.
    if (!groupsCache.length) throw err
    console.warn('[zapo] group refresh failed, serving cached list:', String(err?.message || err))
  }
  return groupsCache
}

// ---------------------------------------------------------------------------
// Favorite groups — persisted in favorites.json (NOT inside .auth, so they
// survive logout / session wipes and re-pairing)
// ---------------------------------------------------------------------------
const FAVORITES_FILE = path.join(__dirname, 'favorites.json')
let favorites = new Set()
try {
  favorites = new Set(JSON.parse(fs.readFileSync(FAVORITES_FILE, 'utf8')))
} catch {} // first run — no file yet

function saveFavorites () {
  try {
    fs.writeFileSync(FAVORITES_FILE, JSON.stringify([...favorites], null, 2))
  } catch (err) {
    console.warn('[favorites] save failed:', String(err?.message || err))
  }
}

// ---------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------
const app = express()
app.use(express.json())
app.use(express.static(path.join(__dirname, 'public')))

// Media uploads staged to a temp dir (multer creates it), deleted after the send
const upload = multer({
  dest: path.join(os.tmpdir(), 'zapo-api-uploads'),
  limits: { fileSize: 100 * 1024 * 1024 } // 100 MB cap
})

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
    // Merge the favorite flag at response time so starring never needs a group refresh.
    // Favorites first; sort is stable so the original order is kept within each half.
    const merged = groups
      .map(g => ({ ...g, favorite: favorites.has(g.jid) }))
      .sort((a, b) => (b.favorite ? 1 : 0) - (a.favorite ? 1 : 0))
    res.json({ groups: merged })
  } catch (err) {
    res.status(500).json({ error: String(err?.message || err) })
  }
})

// Mark / unmark a group as favorite
app.post('/api/favorites', (req, res) => {
  const { jid, favorite } = req.body || {}
  if (typeof jid !== 'string' || !jid.endsWith('@g.us')) {
    return res.status(400).json({ error: 'jid must be a group JID ending in @g.us' })
  }
  if (favorite) favorites.add(jid)
  else favorites.delete(jid)
  saveFavorites()
  res.json({ ok: true, favorite: !!favorite })
})

// Send a text or media (image/video) message — ONLY to groups that exist in the
// session (parallel fan-out). Media arrives as multipart/form-data in the `media`
// field; thumbnails are generated automatically by the client's media processor.
app.post('/api/send', upload.single('media'), async (req, res) => {
  let { groupJids, groupJid, message, fast } = req.body || {}
  // multipart fields arrive as strings — normalize both transports
  if (typeof groupJids === 'string') {
    try { groupJids = JSON.parse(groupJids) } catch { groupJids = [] }
  }
  fast = fast === true || fast === 'true'
  const uploadPath = req.file?.path || null
  const discardUpload = () => { if (uploadPath) fs.promises.unlink(uploadPath).catch(() => {}) }

  if (state.status !== 'connected') {
    discardUpload()
    return res.status(409).json({ error: 'Session is not connected yet' })
  }
  // Accept an array of JIDs, or a single one (backward compatible), de-duplicated
  const targets = Array.isArray(groupJids) ? groupJids : (groupJid ? [groupJid] : [])
  const jids = [...new Set(targets)]
  if (!jids.length) {
    discardUpload()
    return res.status(400).json({ error: 'Select at least one group' })
  }
  if (jids.some(j => typeof j !== 'string' || !j.endsWith('@g.us'))) {
    discardUpload()
    return res.status(400).json({ error: 'Every target must be a group JID ending in @g.us' })
  }
  const text = typeof message === 'string' ? message.trim() : ''
  if (!text && !req.file) {
    return res.status(400).json({ error: 'Type a message or attach an image/video' })
  }

  // Classify the attachment by magic bytes (never trust the client mimetype alone)
  let media = null // { kind: 'image'|'video', mime }
  if (req.file) {
    const ft = await fileTypeFromFile(uploadPath).catch(() => null)
    const mime = ft?.mime || req.file.mimetype || ''
    if (mime === 'image/gif') {
      discardUpload()
      return res.status(400).json({ error: 'GIFs are not supported — convert to MP4 video first' })
    }
    const kind = mime.startsWith('image/') ? 'image' : mime.startsWith('video/') ? 'video' : null
    if (!kind) {
      discardUpload()
      return res.status(400).json({ error: 'Only images and videos are supported (got ' + (mime || 'unknown') + ')' })
    }
    media = { kind, mime }
  }

  try {
    // Validate against the warm in-memory group set (refreshed on connect + via the UI).
    // Only touch the network if we have never loaded groups — never block a send otherwise.
    if (!groupsCache.length) await refreshGroups(true)
    const known = new Set(groupsCache.map(g => g.jid))
    const nameOf = new Map(groupsCache.map(g => [g.jid, g.subject]))

    // Retry only on 'client is not connected' — that guarantees the message never left this
    // machine (socket died / keepalive reconnect in progress), so a retry can't duplicate.
    const RETRYABLE_SEND_ERROR = /not connected/i
    const startedAt = Date.now()

    // Pre-process + pre-upload media ONCE and reuse the descriptor for every group.
    // Without this every group send re-runs sharp/ffmpeg and re-encrypts + re-uploads
    // the whole file to the CDN (N full uploads for N groups). Thumbnail edge (100px)
    // and proto shape mirror the library's own media builder exactly.
    let mediaProto = null
    if (media) {
      const isVideo = media.kind === 'video'
      const [uploaded, thumb, probe] = await Promise.all([
        client.message.upload(uploadPath, { type: media.kind, mimetype: media.mime }),
        isVideo
          ? mediaProcessor.generateVideoThumbnail?.(uploadPath, 100).catch(() => null)
          : mediaProcessor.generateImageThumbnail?.(uploadPath, 100).catch(() => null),
        isVideo ? mediaProcessor.probeMedia?.(uploadPath).catch(() => null) : null
      ])
      const common = {
        url: uploaded.url,
        directPath: uploaded.directPath,
        mediaKey: uploaded.mediaKey,
        fileSha256: uploaded.fileSha256,
        fileEncSha256: uploaded.fileEncSha256,
        fileLength: uploaded.fileLength,
        mediaKeyTimestamp: uploaded.mediaKeyTimestamp,
        mimetype: media.mime,
        ...(text ? { caption: text } : {}),
        ...(thumb ? { jpegThumbnail: thumb.jpegThumbnail } : {})
      }
      mediaProto = isVideo
        ? {
            videoMessage: {
              ...common,
              ...(probe?.durationSeconds !== undefined ? { seconds: Math.floor(probe.durationSeconds) } : {}),
              ...(probe?.width !== undefined ? { width: probe.width } : {}),
              ...(probe?.height !== undefined ? { height: probe.height } : {})
            }
          }
        : {
            imageMessage: {
              ...common,
              ...(thumb ? { width: thumb.width, height: thumb.height } : {})
            }
          }
    }

    // linkPreview:false avoids a blocking URL-metadata fetch when a message contains a link.
    const sendOne = async (jid) => {
      if (!known.has(jid)) throw new Error('Not available in the current session')
      const t0 = Date.now()
      const content = mediaProto ?? { type: 'text', text, linkPreview: false }
      for (let attempt = 0; ; attempt += 1) {
        try {
          const result = await client.message.send(jid, content)
          return { result, ms: Date.now() - t0, retried: attempt > 0 }
        } catch (err) {
          if (attempt >= 3 || !RETRYABLE_SEND_ERROR.test(String(err?.message || err))) throw err
          // give the keepalive detection (20s) + reconnect a chance, then try again
          await new Promise(resolve => setTimeout(resolve, (attempt + 1) * 5000))
        }
      }
    }

    // Batch system: SEND_BATCH_SIZE groups in parallel per batch, SEND_BATCH_DELAY_MS between
    // batches. Results are kept in jids order so finalize() maps each back to its group.
    const runBatched = async () => {
      const settled = new Array(jids.length)
      for (let start = 0; start < jids.length; start += SEND_BATCH_SIZE) {
        const slice = jids.slice(start, start + SEND_BATCH_SIZE)
        const batch = await Promise.allSettled(slice.map(sendOne))
        for (let k = 0; k < batch.length; k += 1) settled[start + k] = batch[k]
        if (start + SEND_BATCH_SIZE < jids.length) {
          await new Promise(resolve => setTimeout(resolve, SEND_BATCH_DELAY_MS))
        }
      }
      return settled
    }

    const finalize = (settled) => {
      discardUpload() // all batches settled — the staged temp file is no longer needed
      const report = settled.map((r, i) => {
        const jid = jids[i]
        const subject = nameOf.get(jid) || jid
        return r.status === 'fulfilled'
          ? { jid, subject, ok: true, ms: r.value.ms, retried: r.value.retried || undefined }
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
      // ⚡ Fast mode: reply instantly, then run the batches in the background.
      lastSendReport = null
      res.json({ ok: true, fast: true, total: jids.length, batchSize: SEND_BATCH_SIZE, ms: Date.now() - startedAt })
      runBatched().then(finalize).catch(() => { discardUpload() })
      return
    }

    const settled = await runBatched()
    const { sent, failed, total, ms, report } = finalize(settled)
    res.json({ ok: sent > 0, sent, failed, total, ms, report })
  } catch (err) {
    discardUpload()
    res.status(500).json({ error: String(err?.message || err) })
  }
})

// Ack report of the most recent send — fast mode polls this to confirm delivery
app.get('/api/last-send', (req, res) => {
  res.json(lastSendReport || { pending: true })
})

// JSON error responses for middleware failures (e.g. multer's 100 MB file cap)
app.use((err, req, res, next) => {
  if (res.headersSent) return next(err)
  res.status(err.statusCode || 500).json({ error: String(err?.message || err) })
})

// Unlink the device and delete the session
app.post('/api/logout', async (req, res) => {
  try {
    const c = client
    if (c) {
      // Best-effort server-side unpair. Never let it hang the request: if the socket
      // is half-dead the IQ can stall, and the local wipe below works regardless.
      const unpair = c.logout().catch(err => {
        // teardown noise is expected: the server kills the stream the moment the device
        // is removed, so in-flight queries fail with 'client is not connected'
        console.warn('[zapo] logout finished with teardown noise:', String(err?.message || err))
      })
      await Promise.race([unpair, new Promise(resolve => setTimeout(resolve, 10_000))])
    }
    // Local teardown + session file deletion always runs, even if unpair failed —
    // this is what guarantees a clean slate and a fresh QR.
    await hardResetSession()
    res.json({ ok: true })
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

// Graceful shutdown — flush the write-behind store and close the sqlite handle
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    try { if (client) await client.disconnect() } catch {}
    try { if (store) await store.destroy() } catch {}
    process.exit(0)
  })
}
