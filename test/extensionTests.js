'use strict';

require('should');
var mysql = require('mysql');
var config = require('config');

describe('extensions', function(){
	describe('#testPoolProperties()', function(){
		it('should return without error', function(done){

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
});