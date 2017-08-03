import * as mysql from 'mysql';
import MySQL from './mysql';
import actions from './actions';
import { IConnection, IConnectionConfig as IMySQLConnectionConfig } from 'mysql';

export interface IConnectionConfig extends IMySQLConnectionConfig {
	noAutoCommit?: boolean;
	connection?: IConnection;
}

/**
 * Manages MySQL
 */
class MySQLConnection extends MySQL {

	/**
	 * Perform queries on a connection
	 */
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

	/**
	 * Perform queries within a transaction
	 */
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

	/**
	 * Constructor
	 */
	constructor (options: IConnectionConfig) {

		super(options);

		if (options.connection) {

			this._connection = options.connection;

		} else {

			this._connection = mysql.createConnection(options);

		}

	}

	/**
	 * Begin a transaction
	 */
	public async begin (): Promise<void> {

		return actions.begin(this._connection);

	}

	/**
	 * Commit a transaction
	 */
	public async commit (): Promise<void> {

		return actions.commit(this._connection);

	}

	/**
	 * Rollback a transaction
	 */
	public async rollback (): Promise<void> {

		return actions.rollback(this._connection);

	}

	/**
	 * Format a query using the underlying connection
	 */
	public format (query: string, args: Array<string | number>): string {

		return this._connection.format(query, args);

	}

	/**
	 * Get the tables in the db
	 */
	public async tables (db: string): Promise<string[]> {

		return actions.tables(this._connection, db);

	}

	/**
	 * Query
	 */
	public async query (query: string, queryArgs: any[] = []): Promise<any[]> {

		try {

			return actions.query(this._connection, query, queryArgs);

		} catch (ex) {

			this.error('query', {err: ex});
			throw ex;

		}

	}

	/**
	 * Query all the records
	 */
	public async queryAll (db: string, table: string): Promise<any[]> {

		return actions.queryAll(this._connection, db, table);

	}

	/**
	 * Query stream
	 */
	public async queryStream (
		query: string,
		delegate: (record) => Promise<void> | void,
		queryArgs: Array<number | string> = []): Promise<void> {

		return actions.queryStream(this._connection, query, delegate, queryArgs);

	}

	/**
	 * Count all the records in a table
	 */
	public async count (db: string, table: string): Promise<number> {

		return actions.count(this._connection, db, table);

	}

	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 */
	public async writeRecord (db: string, table: string, record: any): Promise<void> {

		return actions.writeRecord(this._connection, db, table, record);

	}

	/**
	 * Dispose the connection
	 */
	public async dispose (): Promise<void> {

		await super.dispose();
		await actions.dispose(this._connection);

	}

}

export default MySQLConnection;
