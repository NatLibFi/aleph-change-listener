const _ = require('lodash');
const moment = require('moment');
const debug = require('debug')('Z106-Listener');
const utils = require('./utils');


async function getChangesSinceDate(base, connection, sinceDate) {
  debug(`Fetching changes since ${sinceDate}`);
  const date = sinceDate.format('YYYYMMDD');
  const time = sinceDate.format('HHmm');

  const result = await connection.execute('select * from FIN01.z106 where Z106_UPDATE_DATE >= :dateVar AND Z106_TIME >= :timeVar ORDER BY Z106_UPDATE_DATE, Z106_TIME ASC', [date, time], {resultSet: true});


  const rows = await utils.readAllRows(result.resultSet);

  const changes = rows.map(parseZ106Row);


  // since resolution is 1 minutes, persist result to file
  // after fetch, filter out stuff that was persisted 
  // BASE_${base}_STASH


  // filter out ones that have already been given


  // persist new set of stuff that has been given

  return changes;


}

function parseZ106Row(row) {
  const { Z106_REC_KEY, Z106_SECONDARY_KEY, Z106_CATALOGER, Z106_LEVEL, Z106_UPDATE_DATE, Z106_LIBRARY, Z106_TIME } = row;
    
  return {
    recordId: Z106_REC_KEY,
    secondaryKey: Z106_SECONDARY_KEY,
    user: Z106_CATALOGER.trim(),
    changeLevel: Z106_LEVEL,
    date: parseDate(Z106_UPDATE_DATE, Z106_TIME),
    library: Z106_LIBRARY
  };
}


function parseDate(dateString, timeNumber) {
  
  if (dateString.length !== 8) {
    throw new Error(`Incorrect format for aleph date ${dateString}`);
  }

  const timeString = _.padStart(timeNumber.toString(), 4, '0');

  if (timeString.length !== 4) {
    throw new Error(`Incorrect format for aleph time ${timeString}`);
  }
  
  return moment(`${dateString} ${timeString}`, 'YYYYMMDD HHmmssSS');
}

module.exports = {
  getChangesSinceDate
};
