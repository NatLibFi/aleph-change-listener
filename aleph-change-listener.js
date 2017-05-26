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

const CURSOR_SAVE_FILE = '.aleph-changelistener-cursors';

oracledb.outFormat = oracledb.OBJECT;

const POLL_INTERVAL_MS = 5000;

const Z106Bases = ['FIN01', 'FIN10', 'FIN11'];
const Z106CursorKeys = Z106Bases.reduce((keys, base) => _.set(keys, base, `Z106_FOR_${base}`), {});
const Z106Listeners = Z106Bases.reduce((listeners, base) => _.set(listeners, base, Z106Listener.create(base)), {});

const initialCursors = loadCursors();

oracledb.getConnection(dbConfig)
  .then(async connection => {

    if (DEBUG_SQL) {
      utils.decorateConnectionWithDebug(connection);
    }

    // if initialCursors are not initialzied, then check from db for current cursors.
    await Promise.all(Z106Bases.map(async base => {
      if (!initialCursors[Z106CursorKeys[base]]) {
        initialCursors[Z106CursorKeys[base]] = await Z106Listeners[base].getDefaultCursor(connection);
      }
    }));
    
    if (!initialCursors.Z115_cursor) {
      initialCursors.Z115_cursor = await Z115Listener.getDefaultCursor(connection);
    }

    const poller = Poller.create(POLL_INTERVAL_MS, pollAction(connection, initialCursors));
    poller.start();

  }).catch(error => {
    console.log(error);
  });

function pollAction(connection, cursors) {

  let Z115_cursor = cursors.Z115_cursor;

  return async function() {

    debug('Loading changes from Z106');
    const z106changes = await Promise.all(Z106Bases.map(async base => {
      const cursor = cursors[Z106CursorKeys[base]];
      const changes = await Z106Listeners[base].getChangesSinceDate(connection, cursor);
      if (changes.length) {
        cursors[Z106CursorKeys[base]] = _.last(changes).date;  
      }
      return changes;
    }));
    
    const z115changes = await Z115Listener.getChangesSinceId(connection, Z115_cursor);
    
    if (z115changes.length) {
      cursors.Z115_cursor = _.last(z115changes).changeId;
    }
    
    Z106Bases.forEach(base => {
      debug(`Cursor ${Z106CursorKeys[base]} is ${cursors[Z106CursorKeys[base]]}`);
    });
    
    debug(`Z115_cursor is ${Z115_cursor}`);
    
    await handleChanges(_.flatten(z106changes), z115changes);

    // write cursors to file for next startup
    saveCursors(cursors);
    
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

function loadCursors() {
  try {
    const cursors = JSON.parse(fs.readFileSync(CURSOR_SAVE_FILE, 'utf8'));
    Z106Bases.forEach(base => {
      cursors[Z106CursorKeys[base]] = moment(cursors[Z106CursorKeys[base]]);
    });
    return cursors;
  } catch(error) {
    return {}; 
  }
}

function saveCursors(data) {
  fs.writeFileSync(CURSOR_SAVE_FILE, JSON.stringify(data), 'utf8');
}