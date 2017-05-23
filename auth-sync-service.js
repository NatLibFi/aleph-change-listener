// running this requires at least node 7.10.0
/* eslint no-console: 0 */

const _ = require('lodash');
const oracledb = require('oracledb');
const debug = require('debug')('main');
const dbConfig = require('./dbconfig.js');
const moment = require('moment');

const Z115Listener = require('./Z115-listener');
const Z106Listener = require('./Z106-listener');
const utils = require('../sync-tool/utils');
const DEBUG_SQL = process.env.DEBUG_SQL;

oracledb.outFormat = oracledb.OBJECT;

oracledb.getConnection(dbConfig)
  .then(async connection => {

    if (DEBUG_SQL) {
      utils.decorateConnectionWithDebug(connection);
    }

    const date = moment('20170523 16592223', 'YYYYMMDD HHmmssSS');


    const changes = await Z106Listener.getChangesSinceDate('FIN01', connection, date);
    console.log(changes);

    /*
    let changes;
   
    let latest = '033346999';

    setInterval(async () => {
      
      changes = await Z115Listener.getChangesSinceId(connection, latest);
      console.log(changes);
      if (changes.length) {
        latest = _.last(changes).changeId;
      }
    }, 5000);
    */
    

    
  }).catch(error => {
    console.log(error);
  });
