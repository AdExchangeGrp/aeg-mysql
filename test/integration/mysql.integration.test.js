import MySQL from '../../src/mysql';
import config from 'config';
import should from 'should';

const rdsConf = config.get('aeg-mysql');

describe('MySQL', async () => {

	const mysql = new MySQL({
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
	});

	it('should return without error', async () => {

		const result = await mysql.tables('hitpath_import');
		should.exist(result);

		// console.log(result);

	});

	it('should return without error', async () => {

		const result = await mysql.query('SELECT * FROM hitpath_affiliates LIMIT 10');
		should.exist(result);

		// console.log(result);

	});

	it('should return without error', async () => {

		const result = await mysql.queryAll('hitpath_import', 'hitpath_affiliates');
		should.exist(result);

		// console.log(result);

	});

	it('should return without error', async () => {

		await mysql.queryStream('SELECT * FROM hitpath_affiliates LIMIT 10', (record) => {

			// console.log(record);

		});

	});

	it('should return without error', async () => {

		const result = await mysql.count('hitpath_import', 'hitpath_affiliates');
		should.exist(result);
		result.should.be.a.Number;
		result.should.be.greaterThan(0);

		// console.log(result);

	});

	it('should return without error', async () => {

		await mysql.writeRecord('node_test', 'test_1', {id: Math.random(), name: 'test'});

	});

});
