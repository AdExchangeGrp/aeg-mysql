import * as BBPromise from 'bluebird';
import * as _ from 'lodash';
import { IConnection } from 'mysql';
import promisifyQuery from './promisify-query';
import { Segment } from 'aws-xray-sdk';
import { IQueryOptions } from './types';

export default {

	async begin (connection: IConnection, options: IQueryOptions = {}): Promise<void> {

		return promisifyQuery(connection, options.segment)('START TRANSACTION');

	},

	async commit (connection: IConnection, options: IQueryOptions = {}): Promise<void> {

		return promisifyQuery(connection, options.segment)('COMMIT');

	},

	async rollback (connection: IConnection, options: IQueryOptions = {}): Promise<void> {

		return promisifyQuery(connection, options.segment)('ROLLBACK');

	},

	async dispose (connection: IConnection): Promise<void> {

		const end: any = BBPromise.promisify(connection.end, {context: connection});
		await end();

	},

	async tables (connection: IConnection, db: string, options: IQueryOptions = {}): Promise<string[]> {

		const query = connection.format('SHOW FULL TABLES IN ?? WHERE Table_Type = \'BASE TABLE\'', [db]);
		const result = await this.query(connection, query, options);
		return _.map(result, (r) => {

			return _.values(r)[0];

		}) as string[];

	},

	async query (connection: IConnection, query: string, options: IQueryOptions = {}): Promise<any[]> {

		return promisifyQuery(connection, options.segment)(query);

	},

	async queryAll (connection: IConnection, db: string, table: string, options: IQueryOptions = {}): Promise<any[]> {

		const query = connection.format('SELECT * FROM ??.??', [db, table]);
		return this.query(connection, query, options);

	},

	async queryStream (
		connection: IConnection,
		query: string,
		delegate: (record) => Promise<void> | void): Promise<void> {

		return new Promise<void>((resolve, reject) => {

			let streamErr;

			connection.query(query)
				.on('error', (err) => {

					streamErr = err;

				})
				.on('result', (row) => {

					connection.pause();

					try {

						Promise.resolve(delegate(row))
							.then(() => {

								connection.resume();

							})
							.catch((ex) => {

								streamErr = ex;
								connection.resume();

							});

					} catch (ex) {

						streamErr = ex;
						connection.resume();

					}

				})
				.on('end', () => {

					if (streamErr) {

						reject(streamErr);

					} else {

						resolve();

					}

				});

		});

	},

	async count (connection: IConnection, db: string, table: string, options: IQueryOptions = {}): Promise<number> {

		const query = connection.format('SELECT COUNT (1) AS c FROM ??.??', [db, table]);
		const result = await this.query(connection, query, options);
		return result[0].c;

	},

	async writeRecord (
		connection: IConnection,
		db: string,
		table: string,
		record: any,
		options: IQueryOptions = {}): Promise<void> {

		return promisifyQuery
		(connection, options.segment)
		(connection.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record]));

	},

	async withTransaction (connection: IConnection, delegate: (connection: IConnection) => Promise<void>): Promise<void> {

		await this.begin(connection);

		try {

			await delegate(connection);
			await this.commit(connection);

		} catch (ex) {

			await this.rollback(connection);
			throw ex;

		}

	}

};
