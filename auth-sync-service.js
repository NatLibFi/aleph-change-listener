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

oracledb.outFormat = oracledb.OBJECT;

const POLL_INTERVAL_MS = 5000;

const Z106ListenerForFIN001 = Z106Listener.create('FIN01');

let iterators;
try {
  iterators = JSON.parse(fs.readFileSync('.auth-sync-service-iterators', 'utf8'));
  iterators.Z106_iterator = moment(iterators.Z106_iterator);
} catch(error) {
  iterators = {
    Z106_iterator: moment('20170524 16592223', 'YYYYMMDD HHmmssSS'),
    Z115_iterator: '30000'
  };
}

oracledb.getConnection(dbConfig)
  .then(async connection => {

    if (DEBUG_SQL) {
      utils.decorateConnectionWithDebug(connection);
    }

    const poller = Poller.create(POLL_INTERVAL_MS, pollAction(connection, iterators.Z106_iterator, iterators.Z115_iterator));
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
    const data = JSON.stringify({ Z106_iterator, Z115_iterator });
    fs.writeFileSync('.auth-sync-service-iterators', data, 'utf8');
  };
}

async function handleChanges(z106changes, z115changes) {
  debug(`z106changes: ${z106changes.length}, z115changes: ${z115changes.length}`);

  console.log({z106changes, z115changes});
}