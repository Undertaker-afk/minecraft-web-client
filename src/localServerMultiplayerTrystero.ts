import { Duplex } from 'stream'
// @ts-expect-error - trystero uses package exports, handled by the bundler
import { joinRoom } from 'trystero/nostr'
import type { Room } from 'trystero'
import Client from 'minecraft-protocol/src/client'
import { resolveTimeout } from './utils'
import { setLoadingScreenStatus } from './appStatus'
import { miscUiState } from './globalState'

const TRYSTERO_APP_ID = 'minecraft-web-client-p2p'

class CustomDuplex extends Duplex {
  constructor (options, public writeAction) {
    super(options)
  }

  _read () { }

  _write (chunk, encoding, callback) {
    this.writeAction(chunk)
    callback()
  }
}

let trysteroRoom: Room | undefined
let trysteroRoomId: string | undefined

export const getJoinLinkTrystero = () => {
  if (!trysteroRoomId) return
  const url = new URL(window.location.href)
  for (const key of url.searchParams.keys()) {
    url.searchParams.delete(key)
  }
  url.searchParams.set('connectPeer', trysteroRoomId)
  url.searchParams.set('peerVersion', localServer!.options.version)
  url.searchParams.set('peerEngine', 'trystero')
  return url.toString()
}

const copyJoinLinkTrystero = async () => {
  miscUiState.wanOpened = true
  const joinLink = getJoinLinkTrystero()!
  if (navigator.clipboard) {
    await navigator.clipboard.writeText(joinLink)
  } else {
    window.prompt('Copy to clipboard: Ctrl+C, Enter', joinLink)
  }
}

export const openToWanAndCopyJoinLinkTrystero = async (writeText: (text: string) => void, doCopy = true) => {
  if (!localServer) return
  if (trysteroRoom) {
    if (doCopy) await copyJoinLinkTrystero()
    return 'Already opened to wan. Join link copied'
  }

  miscUiState.wanOpening = true

  const roomId = Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2)
  trysteroRoomId = roomId

  try {
    const room = joinRoom({ appId: TRYSTERO_APP_ID }, roomId)
    trysteroRoom = room

    const [sendData, receiveData] = room.makeAction<ArrayBuffer>('mc-data')

    // Track per-peer duplex streams
    const peerStreams = new Map<string, CustomDuplex>()

    room.onPeerJoin((peerId) => {
      const serverDuplex = new CustomDuplex({}, (data: Buffer) => {
        void sendData(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength), peerId)
      })
      peerStreams.set(peerId, serverDuplex)

      const client = new Client(true, localServer.options.version, undefined)
      client.setSocket(serverDuplex)
      localServer._server.emit('connection', client)

      const endConnection = () => {
        serverDuplex.end()
      }
      serverDuplex.on('end', endConnection)
      serverDuplex.on('force-close', endConnection)
      client.on('end', endConnection)
    })

    receiveData((data, peerId) => {
      const stream = peerStreams.get(peerId)
      if (stream) {
        stream.push(Buffer.from(data))
      }
    })

    room.onPeerLeave((peerId) => {
      const stream = peerStreams.get(peerId)
      if (stream) {
        stream.end()
        peerStreams.delete(peerId)
      }
    })

    miscUiState.wanOpened = true
  } catch (err) {
    writeText(err.message || String(err))
    trysteroRoom = undefined
    trysteroRoomId = undefined
    miscUiState.wanOpening = false
    return 'Failed to open to wan (Trystero error)'
  }

  miscUiState.wanOpening = false

  if (doCopy) {
    await copyJoinLinkTrystero()
    return 'Copied join link to clipboard'
  }
  return 'Opened to WAN via Trystero'
}

export const closeWanTrystero = () => {
  void trysteroRoom?.leave()
  trysteroRoom = undefined
  trysteroRoomId = undefined
  miscUiState.wanOpened = false
  return 'Closed WAN'
}

export const connectToPeerTrystero = async (roomId: string) => {
  setLoadingScreenStatus('Connecting via Trystero (Nostr)')

  const room = joinRoom({ appId: TRYSTERO_APP_ID }, roomId)
  const [sendData, receiveData] = room.makeAction<ArrayBuffer>('mc-data')

  await resolveTimeout(new Promise<void>((resolve) => {
    room.onPeerJoin(() => {
      resolve()
    })
  }))

  setLoadingScreenStatus('Connected via Trystero')

  const clientDuplex = new CustomDuplex({}, (data: Buffer) => {
    void sendData(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength))
  })

  receiveData((data) => {
    clientDuplex.push(Buffer.from(data))
  })

  room.onPeerLeave(() => {
    clientDuplex.end()
    bot.emit('end', 'Disconnected.')
  })

  return clientDuplex
}
