import MySQLConnection, { IConnectionConfig } from '../../src/mysql-connection';
import * as config from 'config';
import * as should from 'should';
import * as _ from 'lodash';
import { Segment, XrayLogger } from '@adexchange/aeg-xray';
import logger from '@adexchange/aeg-logger';

XrayLogger.initialize(logger);

let segment: Segment | null = null;
let options: IConnectionConfig | null;
let mysqlConnection: MySQLConnection | null = null;

before(async () => {

	segment = new Segment('test');

	const rdsConf: any = config.get('aeg-mysql');

	options = {
		host: rdsConf.host,
		user: rdsConf.user,
		password: rdsConf.password,
		database: 'hitpath',
		insecureAuth: true,
		timezone: 'Z',
		segment,
		emitProgress: true
	};

	mysqlConnection = new MySQLConnection(options);

});

after(async () => {

	segment!.close();
	await mysqlConnection!.dispose();

});

describe('MySQLConnection', async () => {

	it('tables', async () => {

		const result = await mysqlConnection!.tables('hitpath');
		should.exist(result);

	});

	it('query', async () => {

		const result = await mysqlConnection!.query('SELECT * FROM hits_sales LIMIT 10');
		should.exist(result);

	});

	it('count', async () => {

		const result = await mysqlConnection!.count('hitpath', 'state');
		should.exist(result);
		should(result).be.instanceOf(Number);
		should(result).be.greaterThan(0);

	});

	it('clear test', async () => {

		await mysqlConnection!.query('truncate table node_test.test_1');

	});

	it('writeRecord', async () => {

		await mysqlConnection!.writeRecord('node_test', 'test_1', {id: 0, name: 'test'});

		const result = await mysqlConnection!.query('select * from node_test.test_1');
		should.exist(result);
		should(result.length).be.equal(1);

	});

	it('withTransaction static - success', async () => {

		await MySQLConnection.withTransaction(async (mysql) => {

			await mysql.query('select * from node_test.test_1');
			await mysql.writeRecord('node_test', 'test_1', {id: 1, name: 'test2'});
			await mysql.writeRecord('node_test', 'test_1', {id: 2, name: 'test3'});
			await mysql.writeRecord('node_test', 'test_1', {id: 3, name: 'test4'});
			await mysql.query('select * from node_test.test_1');

		}, options!);

		const result = await mysqlConnection!.query('select * from node_test.test_1');
		should.exist(result);
		should(result.length).be.equal(4);

	});

	it('withTransaction static - fail', async () => {

		try {

			await MySQLConnection.withTransaction(async (mysql) => {

				await mysql.writeRecord('node_test', 'test_1', {id: 4, name: 'test5'});
				await mysql.writeRecord('node_test', 'test_1', {id: 5, name: 'test6'});
				await mysql.writeRecord('node_test', 'test_1', {id: 6, name: 'test7'});
				throw new Error('kill it');

			}, options!);

		} catch (ex) {

			// should be invoked

		}

		const result = await mysqlConnection!.query('select * from node_test.test_1');
		should.exist(result);
		should(result.length).be.equal(4);

	});

	it('withConnection', async () => {

		await MySQLConnection.withConnection(async (mysql) => {

			const result = await mysql!.query('select * from node_test.test_1');
			should.exist(result);

		}, options!);

	});

	it('withConnection and no autocommit', async () => {

		const r = await MySQLConnection.withConnection(async (mysql) => {

			const result = await mysql!.query('select * from node_test.test_1');
			should.exist(result);
			return result;

		}, _.extend({noAutoCommit: true}, options));

		should.exist(r);
		r.should.be.an.Array;
		r.length.should.be.greaterThan(0);

	});

});
