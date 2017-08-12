import * as mysql from 'mysql';
import { MySQL } from './mysql';
import actions from './actions';
import { IConnection, IConnectionConfig as IMySQLConnectionConfig } from 'mysql';
import { Segment } from '@adexchange/aeg-xray';

export interface IConnectionConfig extends IMySQLConnectionConfig {
	noAutoCommit?: boolean;
	connection?: IConnection;
	segment?: Segment;
}

class MySQLConnection extends MySQL {

	public static async withConnection (
		delegate: (connection: MySQLConnection) => Promise<any> | any,
		options: IConnectionConfig = {}): Promise<any> {

		// noinspection JSCheckFunctionSignatures
		const mysqlConnection = new MySQLConnection(options);

		if (options.noAutoCommit) {

			await mysqlConnection.query('SET autocommit=0');

		}

		const result = await Promise.resolve(delegate(mysqlConnection));

		if (options.noAutoCommit) {

			await mysqlConnection.query('COMMIT');

		}

		await mysqlConnection.dispose();

		return result;

	}

	public static async withTransaction (
		delegate: (connection: MySQLConnection) => Promise<void> | void,
		options: IConnectionConfig = {}): Promise<void> {

		const mysqlConnection = new MySQLConnection(options);

		await mysqlConnection.begin();

		try {

			await Promise.resolve(delegate(mysqlConnection));
			await mysqlConnection.commit();

		} catch (ex) {

			await mysqlConnection.rollback();
			throw ex;

		} finally {

			// do not close the connection if its not managed here
			if (!options.connection) {

				await mysqlConnection.dispose();

			}

		}

	}

	private _connection: IConnection;

	private _segment: Segment | undefined;

	constructor (options: IConnectionConfig) {

		super(options);

		this._segment = options.segment;

		if (options.connection) {

			this._connection = options.connection;

		} else {

			this._connection = mysql.createConnection(options);

		}

	}

	public async begin (): Promise<void> {

		return actions.begin(this._connection, {segment: this._segment});

	}

	public async commit (): Promise<void> {

		return actions.commit(this._connection, {segment: this._segment});

	}

	public async rollback (): Promise<void> {

		return actions.rollback(this._connection, {segment: this._segment});

	}

	public format (query: string, args: any[] = []): string {

		return this._connection.format(query, args);

	}

	public async tables (db: string): Promise<string[]> {

		return actions.tables(this._connection, db, {segment: this._segment});

	}

	public async query (query: string): Promise<any[]> {

		try {

			return actions.query(this._connection, query, {segment: this._segment});

		} catch (ex) {

			this.error('query', {err: ex});
			throw ex;

		}

	}

	public async queryWithArgs (query: string, args: any[]): Promise<any[]> {

		return this.query(this.format(query, args));

	}

	public async count (db: string, table: string): Promise<number> {

		return actions.count(this._connection, db, table, {segment: this._segment});

	}

	public async writeRecord (db: string, table: string, record: any): Promise<void> {

		return actions.writeRecord(this._connection, db, table, record, {segment: this._segment});

	}

	public async dispose (): Promise<void> {

		await super.dispose();
		await actions.dispose(this._connection);

	}

}

export default MySQLConnection;
