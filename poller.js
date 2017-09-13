const debug = require('debug')('Poller');

function create(interval, fn) {
  let isRunning = false;
  let timeoutHandle;

  let currentAction;

  async function poll() {
    isRunning = true;
    currentAction = fn();
    await currentAction;

    if (isRunning) {
      debug(`Waiting ${interval}ms before next poll`);
      timeoutHandle = setTimeout(poll, interval);
    }
  }

  async function stop() {
    clearTimeout(timeoutHandle);
    isRunning = false;
    await currentAction;
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
