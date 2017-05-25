const debug = require('debug')('Poller');

function create(interval, fn) {

  let timeoutHandle;

  async function poll() {
    await fn();
    debug(`Waiting ${interval}ms before next poll`);
    timeoutHandle = setTimeout(poll, interval);
  }

  function stop() {
    clearTimeout(timeoutHandle);
    debug('Poller stopped');
  }

  return {
    start: poll,
    stop
  };
}

module.exports = {
  create
};
