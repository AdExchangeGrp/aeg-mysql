'use strict';

require('should');
var config = require('config');
var extensions = require('../../lib/extensions');

describe('extensions', function () {

	describe('#testPoolProperties()', function () {

		it('should return without error', function (done) {

			var rdsConf = config.get('RDS');

			var rdsPool = extensions.createPool({
				connectionLimit: 100,
				host: rdsConf.host,
				user: rdsConf.user,
				password: rdsConf.password,
				database: rdsConf.db,
				insecureAuth: true,
				acquireTimeout: 120000,
				waitForConnections: true,
				queueLimit: 0,
				timezone: 'Z'
			});

			rdsPool.should.have.propertyByPath('config', 'connectionConfig', 'database').eql(rdsConf.db);

			done();
		});

	});

	describe.skip('#queryStream()', function () {

		it('should return without error', function (done) {

			var rdsConf = config.get('RDS');

			var rdsPool = extensions.createPool({
				connectionLimit: 100,
				host: rdsConf.host,
				user: rdsConf.user,
				password: rdsConf.password,
				database: rdsConf.db,
				insecureAuth: true,
				acquireTimeout: 120000,
				waitForConnections: true,
				queueLimit: 0,
				timezone: 'Z'
			});

			extensions.queryStream(rdsPool, 'select * from transaction limit 1', null,
				function (record, callback) {
					console.log(record);
					callback();
				},
				function (err) {
					done(err);
				});
		});

	});

});