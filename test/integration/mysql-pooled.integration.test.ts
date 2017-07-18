import MySQLPooled from '../../src/mysql-pooled';
import * as config from 'config';
import * as should from 'should';
import { IConnectionOptions } from "../../src/mysql-connection";

describe('MySQLPooled', async () => {

	let mysql: MySQLPooled | null = null;
	let options: IConnectionOptions | null = null;

	before(() => {

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

		mysql = new MySQLPooled(options);

	});

	after(async () => {

		await mysql!.dispose();

	});

	it('tables', async () => {

		const result = await mysql!.tables('hitpath');
		should.exist(result);

	});

	it('query', async () => {

		const result = await mysql!.query('SELECT * FROM hits_sales LIMIT 10');
		should.exist(result);

	});

	it('queryAll', async () => {

		const result = await mysql!.queryAll('hitpath', 'state');
		should.exist(result);

	});

	it('queryStream', async () => {

		await mysql!.queryStream('SELECT * FROM hits_sales LIMIT 10', (record) => {

			should.exist(record);

		});

	});

	it('count', async () => {

		const result = await mysql!.count('hitpath', 'state');
		should.exist(result);
		should(result).be.instanceOf(Number);
		should(result).be.greaterThan(0);

	});

	it('clear test', async () => {

		await mysql!.query('truncate table node_test.test_1');

	});

	it('writeRecord', async () => {

		await mysql!.writeRecord('node_test', 'test_1', {id: 0, name: 'test'});

		const result = await mysql!.queryAll('node_test', 'test_1');
		should.exist(result);
		should(result.length).be.equal(1);

	});

	it('withTransaction static - success', async () => {

		await mysql!.withTransaction(async (connection) => {

			await connection.query('select * from node_test.test_1');
			await connection.writeRecord('node_test', 'test_1', {id: 1, name: 'test2'});
			await connection.writeRecord('node_test', 'test_1', {id: 2, name: 'test3'});
			await connection.writeRecord('node_test', 'test_1', {id: 3, name: 'test4'});
			await connection.query('select * from node_test.test_1');

		});

		const result = await mysql!.queryAll('node_test', 'test_1');
		should.exist(result);
		should(result.length).be.equal(4);

	});

	it('withTransaction static - fail', async () => {

		try {

			await mysql!.withTransaction(async (connection) => {

				await connection.writeRecord('node_test', 'test_1', {id: 4, name: 'test5'});
				await connection.writeRecord('node_test', 'test_1', {id: 5, name: 'test6'});
				await connection.writeRecord('node_test', 'test_1', {id: 6, name: 'test7'});
				throw new Error('kill it');

			});

		} catch (ex) {

			// should be invoked

		}

		const result = await mysql!.queryAll('node_test', 'test_1');
		should.exist(result);
		should(result.length).be.equal(4);

	});

});
