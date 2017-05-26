const moment = require('moment');
const _ = require('lodash');
const debug = require('debug')('Z115-Listener');
const utils = require('./utils');

const STATUS = {
  LOW_ADD: 'LOW_ADD',
  UPDATE: 'UPDATE',
  LOW_DELETE: 'LOW_DELETE'
};


async function getChangesSinceId(connection, sinceChangeId) {  
  debug(`Fetching changes since ${sinceChangeId}`);
  
  const result = await connection.execute('select * from FIN00.Z115 where Z115_REC_KEY > :sinceChangeId', [sinceChangeId], {resultSet: true});
  
  const rows = await utils.readAllRows(result.resultSet);

  const changes = rows.map(parseZ115Row);

  return compactChanges(changes);
  
}

async function getChangesSinceDate(connection, sinceDate) {
  debug(`Fetching changes since ${sinceDate}`);
  const date = sinceDate.format('YYYYMMDD');
  const time = sinceDate.format('HHmmssSS');
  const result = await connection.execute('select /*+ INDEX(Z115_today_date Z115_DATE_ID) INDEX(Z115_today_time Z115_TIME_ID) */ * from FIN00.Z115 where Z115_today_date >= :datevar and z115_today_time > :timevar ORDER BY Z115_today_date, z115_today_time ASC', [date, time], {resultSet: true});
  
  const rows = await utils.readAllRows(result.resultSet);

  const changes = rows.map(parseZ115Row);

  return compactChanges(changes);
  
}

function compactChanges(changes) {

  const grouped = _.groupBy(changes, change => {
    const dateStr = change.date.format('YYYYMMDD-HHmmssSS');
    return `${dateStr}-${change.recordId}`;
  });
  
  const compacted = _.values(grouped)
    .map(changes => {
      return changes.map(change => {
        change.lowTag = [change.lowTag];
        return change;
      });
    })
    .map(changes => changes.reduce((acc, change) => {      
      acc.lowTag = _.concat(acc.lowTag, change.lowTag);
      return acc;
    }));
    
  return compacted.sort((a, b) => a.date - b.date);
}

async function getDefaultCursor(connection) {
  debug('Querying default value for the cursor.');
  const result = await connection.execute('select max(Z115_REC_KEY) as CHANGEID from FIN00.Z115', [], {resultSet: true});
  const latestChangeRow = await result.resultSet.getRow();
  const latestChangeId = latestChangeRow.CHANGEID;
  return latestChangeId;
}

function parseStatus(statusCode) {
  switch (statusCode) {
    case 'N': return STATUS.LOW_ADD;
    case 'C': return STATUS.UPDATE;
    case 'D': return STATUS.LOW_DELETE;
  }
}

function parseZ115Row(row) {
  const { Z115_REC_KEY, Z115_LIBRARY, Z115_OWN, Z115_STATUS, Z115_TODAY_DATE, Z115_TODAY_TIME, Z115_NO_LINES, Z115_TAB } = row;

  // Z115_TAB on tietueen 001.

  return {
    changeId: Z115_REC_KEY,
    library: Z115_LIBRARY,
    lowTag: Z115_OWN.trim(),
    status: parseStatus(Z115_STATUS),
    date: parseDate(Z115_TODAY_DATE, Z115_TODAY_TIME),
    noLines: Z115_NO_LINES,
    recordId: Z115_TAB
  };

}

function parseDate(dateNumber, timeString) {
  const dateString = dateNumber.toString();
  if (dateString.length !== 8) {
    throw new Error(`Incorrect format for aleph date ${dateString}`);
  }

  const trimmedTimeString = timeString.trim();
  
  if (trimmedTimeString.length !== 8) {
    throw new Error(`Incorrect format for aleph time ${trimmedTimeString}`);
  }
  
  return moment(`${dateString} ${trimmedTimeString}`, 'YYYYMMDD HHmmssSS');
}

module.exports = {
  STATUS,
  getChangesSinceId,
  getChangesSinceDate,
  getDefaultCursor
};
