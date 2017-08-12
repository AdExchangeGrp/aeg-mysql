import * as BBPromise from 'bluebird';
import * as _ from 'lodash';
import { IConnection } from 'mysql';
import queryWrapper from './query-wrapper';
import { IQueryOptions } from './types';

export default {

	async begin (connection: IConnection, options: IQueryOptions = {}): Promise<void> {

		return queryWrapper(connection, 'START TRANSACTION', options.segment);

	},

	async commit (connection: IConnection, options: IQueryOptions = {}): Promise<void> {

		return queryWrapper(connection, 'COMMIT', options.segment);

	},

	async rollback (connection: IConnection, options: IQueryOptions = {}): Promise<void> {

		return queryWrapper(connection, 'ROLLBACK', options.segment);

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

		return queryWrapper(connection, query, options.segment);

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

		return queryWrapper(
			connection,
			connection.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record]),
			options.segment);

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
