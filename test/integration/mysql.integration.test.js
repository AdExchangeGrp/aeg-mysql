import MySQL from '../../src/mysql';
import config from 'config';
import should from 'should';
import LoggerMock from './logger-mock';
import _ from 'lodash';

const logger = new LoggerMock();

const rdsConf = config.get('aeg-mysql');

describe('MySQL', async () => {

	const options = {
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

	const mysql = new MySQL(options);

	it('tables', async () => {

		const result = await mysql.tables('hitpath_import');
		should.exist(result);

		// console.log(result);

	});

	it('query', async () => {

		const result = await mysql.query('SELECT * FROM hitpath_affiliates LIMIT 10');
		should.exist(result);

		// console.log(result);

	});

	it('queryAll', async () => {

		const result = await mysql.queryAll('hitpath_import', 'hitpath_affiliates');
		should.exist(result);

		// console.log(result);

	});

	it('queryStream', async () => {

		await mysql.queryStream('SELECT * FROM hitpath_affiliates LIMIT 10', (record) => {

			// console.log(record);

		});

	});

	it('count', async () => {

		const result = await mysql.count('hitpath_import', 'hitpath_affiliates');
		should.exist(result);
		result.should.be.a.Number;
		result.should.be.greaterThan(0);

		// console.log(result);

	});

	it('writeRecord', async () => {

		await mysql.writeRecord('node_test', 'test_1', {id: Math.random(), name: 'test'});

	});

	it('withConnection', async () => {

		// noinspection JSCheckFunctionSignatures
		await MySQL.withConnection(async (mysql) => {

			const result = await mysql.queryAll('hitpath_import', 'hitpath_affiliates');
			should.exist(result);

		}, options);

	});

	it('withConnection and no autocommit', async () => {

		await MySQL.withConnection(async (mysql) => {

			const result = await mysql.queryAll('hitpath_import', 'hitpath_affiliates');
			should.exist(result);

		}, _.extend({noAutoCommit: true}, options));

	});

});
