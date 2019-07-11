const EventEmitter = require('events')
const {Readable, Writable} = require('stream')

module.exports =
function createWrapper (DatArchive) {
  class Hyperdrive extends EventEmitter {
    constructor (storage, key, opts) {
      super()

      if (!(this instanceof Hyperdrive)) return new Hyperdrive(storage, key, opts)
      events.EventEmitter.call(this)

      if (isObject(key)) {
        opts = key
        key = null
      }

      if (!opts) opts = {}
      this._writable = false
      this._version = 0

      this._archive = new DatArchive(key, opts)
      this._readyPromise = this._updateVersion().then(() => {
        this._readyPromise = null
        this.key = Buffer.from(this._archive.url.slice('dat://'.length), 'hex')
      })

      // Need to do this to keep the `version` field up to date
      this.watch(null, () => this._updateVersion())
    }

    async _updateVersion() {
      this._archive.getInfo().then(({isOwner, version}) => {
        this._writable = isOwner
        this._version = version
      })
    }

    get version () {
      return this._version
    }

    get writable () {
      return this._version
    }

    replicate (opts) {
      if (!opts) opts = {}

      // Not possible for now
      // TODO: At least return a stream?
      throw new Error('Unable to replicate')
    }

    checkout (version, opts) {
      if (!opts) opts = {}

      const clone = new Hyperdrive(null, this.key, opts)
      clone._archive = clone._archive.checkout(version, opts)

      return clone
    }

    createDiffStream (version, opts) {
      if (!version) version = 0
      if (typeof version === 'number') version = this.checkout(version)
    }

    download (dir, cb) {
      if (typeof dir === 'function') return this.download('/', dir)
    }

    history (opts) {
    }

    // open -> fd
    open (name, flags, mode, opts, cb) {
      if (typeof mode === 'object' && mode) return this.open(name, flags, 0, mode, opts)
      if (typeof mode === 'function') return this.open(name, flags, 0, mode)
      if (typeof opts === 'function') return this.open(name, flags, mode, null, opts)
    }

    read (fd, buf, offset, len, pos, cb) {

    }

    // TODO: move to ./lib
    createReadStream (name, opts) {
      if (!opts) opts = {}
    }

    readFile (name, opts, cb) {
      if (typeof opts === 'function') return this.readFile(name, null, opts)
      if (typeof opts === 'string') opts = { encoding: opts }
      if (!opts) opts = {}
    }

    createWriteStream (name, opts) {
      if (!opts) opts = {}

      name = normalizePath(name)

      var self = this
      var proxy = duplexify()

      // TODO: support piping through a "split" stream like rabin

      proxy.setReadable(false)
      this._ensureContent(function (err) {
        if (err) return proxy.destroy(err)
        if (self._checkout) return proxy.destroy(new Error('Cannot write to a checkout'))
        if (proxy.destroyed) return

        self._lock(function (release) {
          if (!self.latest || proxy.destroyed) return append(null)

          self.tree.get(name, function (err, st) {
            if (err && err.notFound) return append(null)
            if (err) return append(err)
            if (!st.size) return append(null)
            self.content.clear(st.offset, st.offset + st.blocks, append)
          })

          function append (err) {
            if (err) proxy.destroy(err)
            if (proxy.destroyed) return release()

            // No one should mutate the content other than us
            var byteOffset = self.content.byteLength
            var offset = self.content.length

            self.emit('appending', name, opts)

            // TODO: revert the content feed if this fails!!!! (add an option to the write stream for this (atomic: true))
            var stream = self.content.createWriteStream()

            proxy.on('close', done)
            proxy.on('finish', done)

            proxy.setWritable(stream)
            proxy.on('prefinish', function () {
              var st = {
                mode: (opts.mode || DEFAULT_FMODE) | stat.IFREG,
                uid: opts.uid || 0,
                gid: opts.gid || 0,
                size: self.content.byteLength - byteOffset,
                blocks: self.content.length - offset,
                offset: offset,
                byteOffset: byteOffset,
                mtime: getTime(opts.mtime),
                ctime: getTime(opts.ctime)
              }

              proxy.cork()
              self.tree.put(name, st, function (err) {
                if (err) return proxy.destroy(err)
                self.emit('append', name, opts)
                proxy.uncork()
              })
            })
          }

          function done () {
            proxy.removeListener('close', done)
            proxy.removeListener('finish', done)
            release()
          }
        })
      })

      return proxy
    }

    writeFile (name, buf, opts, cb) {
      if (typeof opts === 'function') return this.writeFile(name, buf, null, opts)
      if (typeof opts === 'string') opts = { encoding: opts }
      if (!opts) opts = {}
      if (typeof buf === 'string') buf = new Buffer(buf, opts.encoding || 'utf-8')
      if (!cb) cb = noop
    }

    mkdir (name, opts, cb) {
      if (typeof opts === 'function') return this.mkdir(name, null, opts)
      if (typeof opts === 'number') opts = { mode: opts }
      if (!opts) opts = {}
      if (!cb) cb = noop
    }

    access (name, opts, cb) {
      if (typeof opts === 'function') return this.access(name, null, opts)
      if (!opts) opts = {}
    }

    exists (name, opts, cb) {
      if (typeof opts === 'function') return this.exists(name, null, opts)
      if (!opts) opts = {}
    }

    lstat (name, opts, cb) {
      if (typeof opts === 'function') return this.lstat(name, null, opts)
      if (!opts) opts = {}
      var self = this
    }

    stat (name, opts, cb) {
      if (typeof opts === 'function') return this.stat(name, null, opts)
      if (!opts) opts = {}
      this.lstat(name, opts, cb)
    }

    readdir (name, opts, cb) {
      if (typeof opts === 'function') return this.readdir(name, null, opts)
    }

    unlink (name, cb) {

    }

    rmdir (name, cb) {
    }

    close (fd, cb) {
    }
  }

  return function hyperdrive(storage, key, opts) {
    return new Hyperdrive(storage, key, opts)
  }
}

function noop () {

}
