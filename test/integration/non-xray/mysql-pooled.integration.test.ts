import MySQLPooled, { IPoolConfig } from '../../../src/mysql-pooled';
import * as config from 'config';
import * as should from 'should';

let options: IPoolConfig | null = null;
let mysqlPool: MySQLPooled | null = null;

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

	mysqlPool = new MySQLPooled(options);

});

after(async () => {

	await mysqlPool!.dispose();

});

describe('MySQLPooled', async () => {

	it('queryStream', async () => {

		await mysqlPool!.queryStream('SELECT * FROM hits_sales LIMIT 10', (record) => {

			should.exist(record);

		});

	});

});
