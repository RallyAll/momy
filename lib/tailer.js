'use strict';

const
  util       = require('util'),
  mongodb    = require('mongodb').MongoClient,
  Timestamp  = require('mongodb').Timestamp,
  MySQL      = require('./mysql.js'),
  defs = require('./defs.js'),
  compare = require('./compare'),
  EventEmitter = require('events');

class RestartEmitter extends EventEmitter {}

/**
 * Tailer
 * @class
 */
class Tailer {
  /**
   * Constructor
   * @param {object} config - configulation options
   */
  constructor (config) {
    this.config = config;
    this.url    = config.src || 'mongodb://localhost:27017/test';
    this.url2   = config.srcop || this.url.replace(/\/\w+(\?|$)/, '/local$1');
    this.dbName = this.url.split(/\/|\?/)[3];
    this.defs   = defs.createDefs(config.collections, this.dbName, config.prefix, config.fieldCase);
    this.lastTs = 0;
    this.mysql  = new MySQL(config.dist);
    this.restartEmitter = new RestartEmitter();
  }

  sync() {
    this.mysql.getSchema(this.config.prefix).then( mysqlList => {

      const mongoList = compare.objectToList(this.config.collections) ;
      const newCollections = compare.findNewCollections({mysql: mysqlList, mongo: mongoList});
      const updatedCollections = compare.findUpdatedCollections({mysql: mysqlList, mongo: mongoList});
      const unchangedCollections = compare.findUnchangedCollections({mysql: mysqlList, mongo: mongoList});

      console.log("All collections");
      console.dir(mongoList.length, { depth: null, colors: true });
      console.log("New collections");
      //console.dir(newCollections, { depth: null, colors: true });
      console.dir(newCollections.length, { depth: null, colors: true });
      console.log("Updated collections");
      console.dir(updatedCollections, { depth: null, colors: true });
      console.dir(updatedCollections.length, { depth: null, colors: true });
      console.log("Unchanged collections");
      //console.dir(unchangedCollections, { depth: null, colors: true });
      console.dir(unchangedCollections.length, { depth: null, colors: true });

      const collectionsToTail = compare.listToObject(unchangedCollections);
      const collectionsToImport = compare.listToObject(newCollections.concat(updatedCollections));

      const defsToImport = defs.createDefs(collectionsToImport, this.dbName, this.config.prefix, this.config.fieldCase); 

      this.defs = defs.createDefs(collectionsToTail, this.dbName, this.config.prefix, this.config.fieldCase); 

      //We start tailing the collections that haven't changed
      //If that number is 0, we might have a problem...
      this.start();

      console.log("Number of collections to import: " + Object.keys(collectionsToImport).length);
      if(Object.keys(collectionsToImport).length > 0){
        console.log("Importing new or changed schemas");
        this.mysql.createTable(defsToImport).then( () => {
          return Promise.all( defsToImport.map( def => {
            console.log("Importing " + def.name);
            return this.importCollection(def).then( () => {
              console.log("DONE");
              this.defs.push(def);
              this.restartEmitter.emit('restart');
            });
          })).then( () => {
            console.log("DONE Importing all Collections");
          });
        });
      }
    });
  }

  /** Start tailing **/
  start () {
    this.mysql.readTimestamp()
      .then(() => this.updateTimestamp(undefined, true))
      .then(() => this.tailForever())
      .catch(err => this.stop(err));
  }

  /** Import all and start tailing **/
  importAndStart () {
    this.mysql.createTable(this.defs)
      .then(() => this.importAll())
      .then(() => this.updateTimestamp())
      .then(() => this.tailForever())
      .catch(err => this.stop(err));
  }

  stop (err) {
    if (err) util.log(err);
    util.log('Bye');
    process.exit();
  }

  /**
   * Import all
   * @returns {Promise} with no value
   */
  importAll () {
    util.log('Begin to import...')
    let promise = Promise.resolve()
    this.defs.forEach(def => {
      promise = promise.then(() => this.importCollection(def))
    })
    promise.then(() => {
      util.log('Done.')
    })
    return promise
  }

  /**
   * Import collection
   * @param {object} def - definition of fields
   * @returns {Promise} with no value
   */
  importCollection (def) {
    util.log(`Import records in ${ def.ns }`)
    return new Promise(resolve =>
      mongodb.connect(this.url, { 'auto_reconnect': true })
        .then(db => {
          const stream = db.collection(def.name).find().stream()
          stream
            .on('data', item => {
              stream.pause()
              this.mysql.insert(def, item, () => stream.resume())
            })
            .on('end', () => {
              resolve()
            })
        }))
  }

  /**
   * Check the latest log in Mongo, then catch the timestamp up in MySQL
   * @param {number} ts - unless null then skip updating in MySQL
   * @param {boolean} skipUpdateMySQL - skip update in MySQL
   * @returns {Promise} with no value
   */
  updateTimestamp (ts, skipUpdateMySQL) {
    if (ts) {
      this.lastTs = ts
      if (!skipUpdateMySQL) this.mysql.updateTimestamp(ts)
      return Promise.resolve()
    }
    return new Promise(resolve =>{
      mongodb.connect(this.url2, { 'auto_reconnect': true })
        .then(db =>{
          db.collection('oplog.rs').find().sort({ $natural: -1 }).limit(1)
            .nextObject()
            .then(item => {
              ts = item.ts.toNumber()
              this.lastTs = ts
              if (!skipUpdateMySQL) this.mysql.updateTimestamp(ts)
              resolve()
            });
        });
    });
  }

  /**
   * Tail forever
   * @returns {Promise} with no value
   */
  tailForever () {
    return new Promise((resolve, reject) => {
      let counter = 0
      let promise = Promise.resolve()
      const chainPromise = () => {
        promise = promise
          .then(() => {
            const message = counter++
              ? 'Reconnecting to MongoDB...'
              : 'Connecting to MongoDB...'
            util.log(message)
            return this.tail()
          })
          .catch(err => reject(err))
          .then(chainPromise)
      }
      chainPromise()
    })
  }

  /**
   * Tail the log of Mongo by tailable cursors
   * @returns {Promise} with no value
   */
  tail () {
    const
      ts  = this.lastTs,
      nss = this.defs.map(def => def.ns),
      filters = {
        ns: { $in: nss },
        ts: { $gt: Timestamp.fromNumber(ts) }
      },
      curOpts = {
        tailable: true,
        timeout: false,
        oplogReplay: true,
        awaitData: true,
        numberOfRetries: 60 * 60 * 24,//Number.MAX_VALUE,
        tailableRetryInterval: 1000
      };

    util.log(`Begin to watch... (from ${ ts })`);
    return new Promise((resolve, reject) =>
      mongodb.connect(this.url2).then(db => {
        const cur = db.collection('oplog.rs').find(filters, curOpts);
        const stream = cur.stream();
        stream.on('data', log => {
            if (log.op == 'n' || log.ts.toNumber() == ts) return;
            this.process(log);
        })
        .on('close', () => {
          util.log('Stream closed....');
          db.close();
          resolve();
        })
        .on('error', err => {
          db.close();
          reject(err);
        });

        this.restartEmitter.on('restart', () => {
          util.log('Restart Event recieved');
          stream.pause();
          db.close();
          resolve();
        });
      }));
  }

  /**
   * Process the log and sync to MySQL
   * @param {object} log - the log retrieved from oplog.rs
   * @returns {undefined}
   */
  process (log) {
    const def = this.defs.filter(def => log.ns == def.ns)[0]
    if (!def) return

    this.updateTimestamp(log.ts.toNumber())
    switch (log.op) {
      case 'i':
        util.log(`Insert a new record into ${ def.ns }`)
        return this.mysql.insert(def, log.o)
      case 'u':
        util.log(`Update a record in ${ def.ns } (${ def.idName }=${ log.o2[def.idName] })`)
        return this.mysql.update(def, log.o2[def.idName], log.o.$set, log.o.$unset)
      case 'd':
        util.log(`Delete a record in ${ def.ns } (${ def.idName }=${ log.o[def.idName] })`)
        return this.mysql.remove(def, log.o[def.idName])
      default:
        return
    }
  }
}

module.exports = Tailer
