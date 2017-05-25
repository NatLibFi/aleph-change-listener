const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const moment = require('moment');
const fs = require('fs');

const Z106Listener = require('./Z106-listener');

const fakeChanges = [
  { Z106_REC_KEY: '008050956',
    Z106_SECONDARY_KEY: 'CHANGE-1',
    Z106_CATALOGER: 'FIX-LINK  ',
    Z106_LEVEL: '30',
    Z106_UPDATE_DATE: '20170523',
    Z106_LIBRARY: 'FIN01',
    Z106_TIME: 1753 },
  { Z106_REC_KEY: '008050956',
    Z106_SECONDARY_KEY: 'CHANGE-2',
    Z106_CATALOGER: 'FIX-LINK  ',
    Z106_LEVEL: '30',
    Z106_UPDATE_DATE: '20170523',
    Z106_LIBRARY: 'FIN01',
    Z106_TIME: 1754 },
  { Z106_REC_KEY: '008050957',
    Z106_SECONDARY_KEY: 'CHANGE-3',
    Z106_CATALOGER: 'FIX-LINK  ',
    Z106_LEVEL: '30',
    Z106_UPDATE_DATE: '20170523',
    Z106_LIBRARY: 'FIN01',
    Z106_TIME: 1754 },
  { Z106_REC_KEY: '008050957',
    Z106_SECONDARY_KEY: 'CHANGE-4',
    Z106_CATALOGER: 'FIX-LINK  ',
    Z106_LEVEL: '30',
    Z106_UPDATE_DATE: '20170523',
    Z106_LIBRARY: 'FIN01',
    Z106_TIME: 1754 },
  { Z106_REC_KEY: '008050933',
    Z106_SECONDARY_KEY: 'CHANGE-5',
    Z106_CATALOGER: 'FIX-LINK  ',
    Z106_LEVEL: '30',
    Z106_UPDATE_DATE: '20170523',
    Z106_LIBRARY: 'FIN01',
    Z106_TIME: 1754 },
];


describe('Z106-Listener', () => {

  let fakeBase = 'XXX01';
  let fakeConnection;
  let getRowStub;
  let fakeDate;

  let z106Listener;
  let sandbox;

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
    sandbox.stub(fs, 'readFileSync').callsFake(sinon.stub().returns('[]'));
    sandbox.stub(fs, 'writeFile').callsFake(sinon.stub().yields(null));

    fakeConnection = {
      execute: sinon.stub()
    };

    z106Listener = Z106Listener.create(fakeBase);

    fakeDate = moment();

    getRowStub = sinon.stub();

    fakeConnection.execute.resolves({
      resultSet: {
        getRow: getRowStub
      }
    });
  });
  afterEach(() => {
    sandbox.restore();
  });

  it('should return list of changes', async () => {
    getRowStub.onCall(0).resolves(fakeChanges[0]);
    getRowStub.onCall(1).resolves(null);

    const changes = await z106Listener.getChangesSinceDate(fakeConnection, fakeDate);
    expect(changes.length).to.equal(1);
  });

  it('should filter out changes that have already been given', async () => {
    getRowStub.onCall(0).resolves(fakeChanges[0]);
    getRowStub.onCall(1).resolves(fakeChanges[1]);
    getRowStub.onCall(2).resolves(null);

    getRowStub.onCall(3).resolves(fakeChanges[1]);
    getRowStub.onCall(4).resolves(fakeChanges[2]);
    getRowStub.onCall(5).resolves(fakeChanges[3]);
    getRowStub.onCall(6).resolves(null);

    const changesAfterFirstRequest = await z106Listener.getChangesSinceDate(fakeConnection, fakeDate);
    expect(_.map(changesAfterFirstRequest, 'secondaryKey')).to.eql(['CHANGE-1', 'CHANGE-2']);

    const changesAfterSecondRequest = await z106Listener.getChangesSinceDate(fakeConnection, fakeDate);
    expect(_.map(changesAfterSecondRequest, 'secondaryKey')).to.eql(['CHANGE-3', 'CHANGE-4']);
    

  });

});
