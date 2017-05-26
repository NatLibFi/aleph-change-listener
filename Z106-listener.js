const fs = require('fs');
const _ = require('lodash');
const moment = require('moment');
const createDebug = require('debug');
const utils = require('./utils');


function create(base, stashPrefix='stash') {
  const debug = createDebug(`Z106-Listener-for${base}`);
  const persistedChangesFilename = `.${stashPrefix}_${base}`;
  let alreadyPassedChanges = readPersistedChanges(persistedChangesFilename);

  async function getChangesSinceDate(connection, sinceDate) {
    debug(`Fetching changes since ${sinceDate}`);
    const date = sinceDate.format('YYYYMMDD');
    const time = sinceDate.format('HHmm');

    const result = await connection.execute(`select * from ${base}.z106 where Z106_UPDATE_DATE >= :dateVar AND Z106_TIME >= :timeVar ORDER BY Z106_UPDATE_DATE, Z106_TIME ASC`, [date, time], {resultSet: true});

    const rows = await utils.readAllRows(result.resultSet);

    const changes = rows.map(parseZ106Row);

    // Since resolution is 1 minute, persist result from last minute to file
    // after fetch, filter out stuff that was persisted 

    const dateOfLastChange = _.get(_.last(changes), 'date');
    const setOfChangesToPersist = _.takeRightWhile(changes, change => dateOfLastChange.isSame(change.date));
    const newChanges = _.differenceWith(changes, alreadyPassedChanges, isEqualChangeObject);
    alreadyPassedChanges = setOfChangesToPersist;
    await writePersistedChanges(persistedChangesFilename, setOfChangesToPersist);

    return _.uniqWith(newChanges, isEqualChangeObject);
  }

  async function getDefaultCursor(connection) {
    debug('Querying default value for the cursor.');
    const result = await connection.execute(`select * from ${base}.z106 ORDER BY Z106_UPDATE_DATE DESC, Z106_TIME DESC`, [], {resultSet: true});
    const latestChangeRow = await result.resultSet.getRow();
    const latestChange = parseZ106Row(latestChangeRow);

    // ensure that the changes from current minute are not returned when cursor is first used.
    const latestChanges = await getChangesSinceDate(connection, latestChange.date);
    return _.last(latestChanges).date;
  }

  return {
    getChangesSinceDate,
    getDefaultCursor
  };
}

function isEqualChangeObject(a, b) {

  return _.isEqual(_.omit(a, 'date'), _.omit(b, 'date')) && a.date.isSame(b.date);
}

function readPersistedChanges(file) {
  try {
    const data = fs.readFileSync(file, 'utf8');
    const changes = JSON.parse(data);
    return changes.map(change => {
      return Object.assign(change, { date: moment(change.date) });
    });
  } catch(error) {
    return [];
  }
}

function writePersistedChanges(file, data) {
  return new Promise((resolve, reject) => {
    fs.writeFile(file, JSON.stringify(data), 'utf8', (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();  
      }
    });
  });
  
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
  create
};
