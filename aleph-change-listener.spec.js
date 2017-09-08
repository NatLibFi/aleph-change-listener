const _ = require('lodash');
const sinon = require('sinon');
const expect = require('chai').expect;
const fs = require('fs');
const AlephChangeListener = require('./aleph-change-listener');
const debug = require('debug')('aleph-change-listener');

const Z115Listener = require('./Z115-listener');
const Z106Listener = require('./Z106-listener');
const Poller = require('./poller');


describe('aleph-change-listener', () => {

  const options = {
    Z106Bases: ['USR00'],
    logger: { log: debug.bind(debug)}
  };
  let fakeConnection;
  let onChangeCb;
  let pollAction;
  let Z115getChangesSinceId;
  let Z116getChangesSinceDate;
  let changesQueueData;
  let sandbox;

  beforeEach(async () => {
    sandbox = sinon.sandbox.create();

    changesQueueData = '[]';
    onChangeCb = sinon.spy();
    Z115getChangesSinceId = sinon.stub();
    Z116getChangesSinceDate = sinon.stub();

    sandbox.stub(fs, 'writeFileSync').callsFake((name, data) => {
      if (name === '.aleph-changelistener-changesqueue') {
        changesQueueData = data;
      }
    });
    sandbox.stub(fs, 'readFileSync').callsFake(() => { return changesQueueData; });


    sandbox.stub(Z115Listener, 'getDefaultCursor').callsFake(() => { });
    sandbox.stub(Z115Listener, 'getChangesSinceId').callsFake(Z115getChangesSinceId);
    sandbox.stub(Z106Listener, 'create').callsFake(() => { 
      return {
        getDefaultCursor: () => {},
        getChangesSinceDate: Z116getChangesSinceDate
      };
    });

    sandbox.stub(Poller, 'create').callsFake((interval, action) => {
      pollAction = action;
      return {
        start: sinon.spy(),
        stop: sinon.spy()
      };
    });

    await AlephChangeListener.create(fakeConnection, options, onChangeCb);

  });
  afterEach(() => sandbox.restore());

  it('First call should keep changes stashed', async () => {
    Z115getChangesSinceId.returns([]);
    Z116getChangesSinceDate.returns({changes: [], nextCursor: null });
    await pollAction();
    expect(onChangeCb.callCount).to.equal(0);
  });

  it('Given two empty polls, it should emit 0 changes', async () => {
    Z115getChangesSinceId.returns([]);
    Z116getChangesSinceDate.returns({changes: [], nextCursor: null });
    await pollAction();
    await pollAction();
    expect(onChangeCb.callCount).to.equal(1);
    expect(onChangeCb.getCall(0).args[0]).to.eql([]);
  });

  it('Given two non-empty polls, it should emit changes from first polling', async () => {
    Z115getChangesSinceId.onCall(0).returns([fakeChange('001', 'lib1')]);
    Z115getChangesSinceId.onCall(1).returns([fakeChange('002', 'lib1')]);
    Z116getChangesSinceDate.returns({changes: [], nextCursor: null });

    await pollAction();
    await pollAction();
    expect(onChangeCb.callCount).to.equal(1);

    const emittedChanges = _.map(onChangeCb.getCall(0).args[0], change => _.pick(change, ['recordId', 'library']));
    expect(emittedChanges).to.eql([ fakeChange('001', 'lib1') ]);
  });

  it('Given two non-empty polls, it should emit changes from first polling and any changes from the second polling if it is about same record', async () => {
    Z115getChangesSinceId.onCall(0).returns([fakeChange('001', 'lib1')]);
    Z116getChangesSinceDate.onCall(0).returns({changes: [], nextCursor: null });
    
    Z115getChangesSinceId.onCall(1).returns([fakeChange('002', 'lib1')]);
    Z116getChangesSinceDate.onCall(1).returns({ changes: [fakeChange('001', 'lib1')], nextCursor: null});
    
    Z115getChangesSinceId.onCall(2).returns([]);
    Z116getChangesSinceDate.onCall(2).returns({changes: [], nextCursor: null });
    
    await pollAction();
    await pollAction();
    await pollAction();
    expect(onChangeCb.callCount).to.equal(2);

    const firstEmittedChanges = _.map(onChangeCb.getCall(0).args[0], change => _.pick(change, ['recordId', 'library']));
    const secondEmittedChanges = _.map(onChangeCb.getCall(1).args[0], change => _.pick(change, ['recordId', 'library']));

    expect(firstEmittedChanges).to.eql([ fakeChange('001', 'lib1') ]);
    expect(secondEmittedChanges).to.eql([ fakeChange('002', 'lib1') ]);


  });

});
function fakeChange(recordId, library) {
  return { recordId, library };
}