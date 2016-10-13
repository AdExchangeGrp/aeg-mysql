import MySQL from '../../src/mysql';
import LoggerMock from './logger-mock';

const logger = new LoggerMock();

describe.skip('MySQL', async () => {

	const mysql = new MySQL({
		logger,
		connectionLimit: 1,
		host: '54.149.152.122',
		user: 'adexchange',
		port: 4040,
		password: '9764da852b919b1ba057f3be293fba4a',
		insecureAuth: true,
		acquireTimeout: 120000,
		waitForConnections: true,
		queueLimit: 0,
		timezone: 'Z',
		dateStrings: true
	});

	it('should return without error', async () => {

		let count = 0;

		try {

			await mysql.queryStream('SELECT * FROM nhl_hits.h_888', () => {

				count++;

				if (count % 1000) {

					console.log(count);

				}

			});

		} catch (ex) {

			console.log(ex);

			console.log('ended', count);

		}

	});

});
