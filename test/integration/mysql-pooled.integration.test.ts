import MySQLPooled from '../../src/mysql-pooled';
import * as config from 'config';
import * as should from 'should';
import { Segment, XrayLogger } from '@adexchange/aeg-xray';
import { IPoolConfig } from 'mysql';
import logger from '@adexchange/aeg-logger';

XrayLogger.initialize(logger);

let segment: Segment | null = null;
let options: IPoolConfig | null = null;
let mysqlPool: MySQLPooled | null = null;
let queryOptions: any;

before(() => {

	segment = new Segment('test');

	queryOptions = {
		segment
	};

	const rdsConf: any = config.get('aeg-mysql');

	options = {
		connectionLimit: 10,
		host: rdsConf.host,
		user: rdsConf.user,
		password: rdsConf.password,
		database: 'hitpath',
		insecureAuth: true,
		acquireTimeout: 120000,
		waitForConnections: true,
		queueLimit: 0,
		timezone: 'Z'
	};

	mysqlPool = new MySQLPooled(options);

});

after(async () => {

	segment!.close();
	await mysqlPool!.dispose();

});

describe('MySQLPooled', async () => {

	it('tables', async () => {

		const result = await mysqlPool!.tables('hitpath', queryOptions);
		should.exist(result);

	});

	it('format', async () => {

		const result = mysqlPool!.format('SELECT * FROM hits_sales LIMIT 10');
		should.exist(result);

	});

	it('query', async () => {

		const result = await mysqlPool!.query('SELECT * FROM hits_sales LIMIT 10', queryOptions);
		should.exist(result);

	});

	it('count', async () => {

		const result = await mysqlPool!.count('hitpath', 'state', queryOptions);
		should.exist(result);
		should(result).be.instanceOf(Number);
		should(result).be.greaterThan(0);

	});

	it('clear test', async () => {

		await mysqlPool!.query('truncate table node_test.test_1', queryOptions);

	});

	it('writeRecord', async () => {

		await mysqlPool!.writeRecord('node_test', 'test_1', {id: 0, name: 'test'}, queryOptions);

		const result = await mysqlPool!.query('select * from node_test.test_1', queryOptions);
		should.exist(result);
		should(result.length).be.equal(1);

	});

	it('withTransaction static - success', async () => {

		await mysqlPool!.withTransaction(async (connection) => {

			await connection.query('select * from node_test.test_1');
			await connection.writeRecord('node_test', 'test_1', {id: 1, name: 'test2'});
			await connection.writeRecord('node_test', 'test_1', {id: 2, name: 'test3'});
			await connection.writeRecord('node_test', 'test_1', {id: 3, name: 'test4'});
			await connection.query('select * from node_test.test_1');

		});

		const result = await mysqlPool!.query('select * from node_test.test_1', queryOptions);
		should.exist(result);
		should(result.length).be.equal(4);

	});

	it('withTransaction static - fail', async () => {

		try {

			await mysqlPool!.withTransaction(async (connection) => {

				await connection.writeRecord('node_test', 'test_1', {id: 4, name: 'test5'});
				await connection.writeRecord('node_test', 'test_1', {id: 5, name: 'test6'});
				await connection.writeRecord('node_test', 'test_1', {id: 6, name: 'test7'});
				throw new Error('kill it');

			});

		} catch (ex) {

			// should be invoked

		}

		const result = await mysqlPool!.query('select * from node_test.test_1', queryOptions);
		should.exist(result);
		should(result.length).be.equal(4);

	});

});
