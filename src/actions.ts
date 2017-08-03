import * as BBPromise from 'bluebird';
import * as _ from 'lodash';
import { IConnection } from 'mysql';

/**
 * MySQL actions
 */
export default {

	/**
	 * Begin a transaction
	 */
	async begin (connection: IConnection): Promise<void> {

		const begin: any = BBPromise.promisify(connection.beginTransaction, {context: connection});
		await begin();

	},

	/**
	 * Commit a transaction
	 */
	async commit (connection: IConnection): Promise<void> {

		const commit: any = BBPromise.promisify(connection.commit, {context: connection});
		await commit();

	},

	/**
	 * Rollback a transaction
	 */
	async rollback (connection: IConnection): Promise<void> {

		const rollback: any = BBPromise.promisify(connection.rollback, {context: connection});
		await rollback();

	},

	/**
	 * End a connection
	 */
	async dispose (connection: IConnection): Promise<void> {

		const end: any = BBPromise.promisify(connection.end, {context: connection});
		await end();

	},

	/**
	 * Get the tables in the db
	 */
	async tables (connection: IConnection, db: string): Promise<string[]> {

		const result = await this.query(connection, 'SHOW FULL TABLES IN ?? WHERE Table_Type = \'BASE TABLE\'', [db]);
		return _.map(result, (r) => {

			return _.values(r)[0];

		}) as string[];

	},

	/**
	 * Query
	 */
	async query (connection: IConnection, query: string, queryArgs: any[] = []): Promise<any[]> {

		const queryAsync: any = BBPromise.promisify(connection.query, {context: connection});
		return queryAsync(connection.format(query, queryArgs));

	},

	/**
	 * Query all the records in a table
	 */
	async queryAll (connection: IConnection, db: string, table: string): Promise<any[]> {

		return this.query(connection, 'SELECT * FROM ??.??', [db, table]);

	},

	/**
	 * Query stream
	 */
	async queryStream (
		connection: IConnection,
		query: string,
		delegate: (record) => Promise<void> | void,
		queryArgs: Array<number | string> = []): Promise<void> {

		return new Promise<void>((resolve, reject) => {

			let streamErr;

			connection.query(connection.format(query, queryArgs))
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

	/**
	 * Count all the records in a table
	 */
	async count (connection: IConnection, db: string, table: string): Promise<number> {

		const result = await this.query(connection, 'SELECT COUNT (1) AS c FROM ??.??', [db, table]);
		return result[0].c;

	},

	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 */
	async writeRecord (connection: IConnection, db: string, table: string, record: any): Promise<void> {

		const query = connection.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record]);
		const queryAsync: any = BBPromise.promisify(connection.query, {context: connection});
		return queryAsync(query);

	},

	/**
	 * Perform queries within a transaction
	 */
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
