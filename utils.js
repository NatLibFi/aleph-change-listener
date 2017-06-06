const debug = require('debug')('utils');

async function readAllRows(resultSet, rows = []) {
  
  const nextRow = await resultSet.getRow();
  if (nextRow === null) {
    await resultSet.close();
    return rows;
  }
  
  rows.push(nextRow);
  return readAllRows(resultSet, rows);
}

module.exports = {
  readAllRows
};
