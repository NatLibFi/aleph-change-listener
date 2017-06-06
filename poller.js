const debug = require('debug')('Poller');

function create(interval, fn) {
  let isRunning = false;
  let timeoutHandle;

  async function poll() {
    isRunning = true;
    await fn();
    if (isRunning) {
      debug(`Waiting ${interval}ms before next poll`);
      timeoutHandle = setTimeout(poll, interval);
    }
  }

  function stop() {
    clearTimeout(timeoutHandle);
    isRunning = false;
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
