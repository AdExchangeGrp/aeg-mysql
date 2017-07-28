import * as mysql from 'mysql';
import * as BBPromise from 'bluebird';
import MySQL from './mysql';
import MySQLConnection from './mysql-connection';
import actions from './actions';
import { IPoolConfig as IMySQLPoolConfig } from 'mysql';

// noinspection TsLint
export interface IPoolConfig extends IMySQLPoolConfig {

}

/**
 * Manages MySQL
 */
class MySQLPooled extends MySQL {

	private _pool: any;

	/**
	 * Constructor
	 */
	constructor (options: IPoolConfig) {

		super(options);

		this._pool = mysql.createPool(options);
		BBPromise.promisifyAll(this._pool);

	}

	/**
	 * Perform queries within a transaction
	 * @param {function} delegate
	 */
	public async withTransaction (delegate: (connection: MySQLConnection) => Promise<void> | void): Promise<void> {

		const connection = await this._pool.getConnectionAsync();
		await MySQLConnection.withTransaction(delegate, {connection});
		connection.release();

	}

	/**
	 * Get the tables in the db
	 */
	public async tables (db: string): Promise<string[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.tables(connection, db);
		connection.release();
		return result;

	}

	/**
	 * Query
	 */
	public async query (query: string, queryArgs: Array<string | number> = []): Promise<any[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.query(connection, query, queryArgs);
		connection.release();
		return result;

	}

	/**
	 * Query all the records
	 */
	public async queryAll (db: string, table: string): Promise<any[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.queryAll(connection, db, table);
		connection.release();
		return result;

	}

	/**
	 * Query stream
	 */
	public async queryStream (
		query: string,
		delegate: (record) => Promise<void> | void,
		queryArgs: Array<number | string> = []): Promise<void> {

		const connection = await this._pool.getConnectionAsync();
		await actions.queryStream(connection, query, delegate, queryArgs);
		connection.release();

	}

	/**
	 * Count all the records in a table
	 */
	public async count (db: string, table: string): Promise<number> {

		const connection = await this._pool.getConnectionAsync();
		const result = await actions.count(connection, db, table);
		connection.release();
		return result;

	}

	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 */
	public async writeRecord (db: string, table: string, record: any): Promise<void> {

		const connection = await this._pool.getConnectionAsync();
		await actions.writeRecord(connection, db, table, record);
		connection.release();

	}

	/**
	 * Dispose the connection when not using a pool
	 */
	public async dispose (): Promise<void> {

		await super.dispose();
		await this._pool.endAsync();

	}

}

export default MySQLPooled;
