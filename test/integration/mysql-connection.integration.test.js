import MySQLConnection from '../../src/mysql-connection';
import config from 'config';
import should from 'should';
import LoggerMock from './logger-mock';
import _ from 'lodash';

describe('MySQLConnection', async () => {

	let mysql = null;
	let options = null;

	before(() => {

		const logger = new LoggerMock();
		const rdsConf = config.get('aeg-mysql');
		options = {
			logger,
			connectionLimit: 10,
			host: rdsConf.host,
			user: rdsConf.user,
			password: rdsConf.password,
			database: rdsConf.db,
			insecureAuth: true,
			acquireTimeout: 120000,
			waitForConnections: true,
			queueLimit: 0,
			timezone: 'Z'
		};

		mysql = new MySQLConnection(options);

	});

	after(async () => {

		await mysql.dispose();

	});

	it('tables', async () => {

		const result = await mysql.tables('hitpath_import');
		should.exist(result);

	});

	it('query', async () => {

		const result = await mysql.query('SELECT * FROM hitpath_affiliates LIMIT 10');
		should.exist(result);

	});

	it('queryAll', async () => {

		const result = await mysql.queryAll('hitpath_import', 'hitpath_affiliates');
		should.exist(result);

	});

	it('queryStream', async () => {

		await mysql.queryStream('SELECT * FROM hitpath_affiliates LIMIT 10', (record) => {

			should.exist(record);

		});

	});

	it('count', async () => {

		const result = await mysql.count('hitpath_import', 'hitpath_affiliates');
		should.exist(result);
		result.should.be.a.Number;
		result.should.be.greaterThan(0);

	});

	it('clear test', async () => {

		await mysql.query('truncate table node_test.test_1');

	});

	it('writeRecord', async () => {

		await mysql.writeRecord('node_test', 'test_1', {id: 0, name: 'test'});

		const result = await mysql.queryAll('node_test', 'test_1');
		should.exist(result);
		result.length.should.be.equal(1);

	});

	it('withTransaction static - success', async () => {

		await MySQLConnection.withTransaction(async (mysql) => {

			await mysql.query('select * from node_test.test_1');
			await mysql.writeRecord('node_test', 'test_1', {id: 1, name: 'test2'});
			await mysql.writeRecord('node_test', 'test_1', {id: 2, name: 'test3'});
			await mysql.writeRecord('node_test', 'test_1', {id: 3, name: 'test4'});
			await mysql.query('select * from node_test.test_1');

		}, options);

		const result = await mysql.queryAll('node_test', 'test_1');
		should.exist(result);
		result.length.should.be.equal(4);

	});

	it('withTransaction static - fail', async () => {

		try {

			await MySQLConnection.withTransaction(async (mysql) => {

				await mysql.writeRecord('node_test', 'test_1', {id: 4, name: 'test5'});
				await mysql.writeRecord('node_test', 'test_1', {id: 5, name: 'test6'});
				await mysql.writeRecord('node_test', 'test_1', {id: 6, name: 'test7'});
				throw new Error('kill it');

			}, options);

		} catch (ex) {

			// should be invoked

		}

		const result = await mysql.queryAll('node_test', 'test_1');
		should.exist(result);
		result.length.should.be.equal(4);

	});

	it('withConnection', async () => {

		await MySQLConnection.withConnection(async (mysql) => {

			const result = await mysql.queryAll('hitpath_import', 'hitpath_affiliates');
			should.exist(result);

		}, options);

	});

	it('withConnection and no autocommit', async () => {

		await MySQLConnection.withConnection(async (mysql) => {

			const result = await mysql.queryAll('hitpath_import', 'hitpath_affiliates');
			should.exist(result);

		}, _.extend({noAutoCommit: true}, options));

	});

});
