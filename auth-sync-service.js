// running this requires at least node 7.10.0
/* eslint no-console: 0 */

const _ = require('lodash');
const oracledb = require('oracledb');
const debug = require('debug')('main');
const dbConfig = require('./dbconfig.js');
const moment = require('moment');
const fs = require('fs');

const Z115Listener = require('./Z115-listener');
const Z106Listener = require('./Z106-listener');
const Poller = require('./poller');
const utils = require('../sync-tool/utils');
const DEBUG_SQL = process.env.DEBUG_SQL;

const ITERATOR_SAVE_FILE = '.auth-sync-service-iterators';

oracledb.outFormat = oracledb.OBJECT;

const POLL_INTERVAL_MS = 5000;

const Z106ListenerForFIN001 = Z106Listener.create('FIN01');

const initialIterators = loadIterators();

oracledb.getConnection(dbConfig)
  .then(async connection => {

    if (DEBUG_SQL) {
      utils.decorateConnectionWithDebug(connection);
    }

    // if initialIterators are not initialzied, then check from db for current iterators.

    const poller = Poller.create(POLL_INTERVAL_MS, pollAction(connection, initialIterators.Z106_iterator, initialIterators.Z115_iterator));
    poller.start();

  }).catch(error => {
    console.log(error);
  });

function pollAction(connection, date, changeId) {
  let Z106_iterator = date;
  let Z115_iterator = changeId;

  return async function() {

    debug('Loading changes from Z106');
    const z106changes = await Z106ListenerForFIN001.getChangesSinceDate(connection, Z106_iterator);
    
    if (z106changes.length) {
      Z106_iterator = _.last(z106changes).date;
    }

    const z115changes = await Z115Listener.getChangesSinceId(connection, Z115_iterator);
    
    if (z115changes.length) {
      Z115_iterator = _.last(z115changes).changeId;
    }
    
    debug(`Z106_iterator is ${Z106_iterator}`);
    debug(`Z115_iterator is ${Z115_iterator}`);
    
    await handleChanges(z106changes, z115changes);

    // write iterators to file for next startup
    saveIterators({ Z106_iterator, Z115_iterator });
    
  };
}

async function handleChanges(z106changes, z115changes) {
  debug(`z106changes: ${z106changes.length}, z115changes: ${z115changes.length}`);

  // merge changes
  // move non common data to metadata
  // common items: recordId, libraty
  // date -> metadata.Z106Date and metadata.Z115Date

  console.log({z106changes, z115changes});
}

function loadIterators() {
  let iterators;
  try {
    iterators = JSON.parse(fs.readFileSync(ITERATOR_SAVE_FILE, 'utf8'));
    iterators.Z106_iterator = moment(iterators.Z106_iterator);    
  } catch(error) {
    // todo, get latests from database if iterators are missing.
    iterators = {
      Z106_iterator: moment('20170524 16592223', 'YYYYMMDD HHmmssSS'),
      Z115_iterator: '30000'
    };
  }
  
  return iterators;
}
function saveIterators(data) {
  fs.writeFileSync(ITERATOR_SAVE_FILE, JSON.stringify(data), 'utf8');
}