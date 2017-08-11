import MySQLConnection, { IConnectionConfig } from '../../../src/mysql-connection';
import * as config from 'config';
import * as should from 'should';

let options: IConnectionConfig | null;
let mysqlConnection: MySQLConnection | null = null;

before(async () => {

	const rdsConf: any = config.get('aeg-mysql');

	options = {
		host: rdsConf.host,
		user: rdsConf.user,
		password: rdsConf.password,
		database: 'hitpath',
		insecureAuth: true,
		timezone: 'Z'
	};

	mysqlConnection = new MySQLConnection(options);

});

after(async () => {

	await mysqlConnection!.dispose();

});

describe('MySQLConnection', async () => {

	it('queryStream', async () => {

		await mysqlConnection!.queryStream('SELECT * FROM affiliates LIMIT 10', (record) => {

			should.exist(record);

		});

	});

});
