'use strict';

require('should');
var mysql = require('mysql');
var config = require('config');
var extensions = require('../lib/extensions');
var logger = require('aeg-logger');

describe('extensions', function () {
	describe('#testPoolProperties()', function () {
		it('should return without error', function (done) {

			var rdsConf = config.get('RDS');

			var rdsPool = mysql.createPool({
				connectionLimit: 100,
				host: rdsConf.host,
				user: rdsConf.user,
				password: rdsConf.password,
				database: rdsConf.db,
				insecureAuth: true,
				acquireTimeout: 120000,
				waitForConnections: true,
				queueLimit: 0
			});

			rdsPool.should.have.propertyByPath('config', 'connectionConfig', 'database').eql(rdsConf.db);

			done();
		});
	});

	describe('#queryStream()', function () {
		it('should return without error', function (done) {

			var rdsConf = config.get('RDS');

			var rdsPool = mysql.createPool({
				connectionLimit: 100,
				host: rdsConf.host,
				user: rdsConf.user,
				password: rdsConf.password,
				database: rdsConf.db,
				insecureAuth: true,
				acquireTimeout: 120000,
				waitForConnections: true,
				queueLimit: 0
			});

			extensions.queryStream(rdsPool, 'select * from transaction limit 1', null,
				function (record) {
					logger.info(record);
				},
				function (err) {
					done(err);
				});
		});
	});
});