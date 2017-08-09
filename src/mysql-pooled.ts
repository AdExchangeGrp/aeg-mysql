import * as mysql from 'mysql';
import * as BBPromise from 'bluebird';
import { MySQL } from './mysql';
import MySQLConnection from './mysql-connection';
import actions from './actions';
import { IPoolConfig as IMySQLPoolConfig } from 'mysql';

export interface IPoolConfig extends IMySQLPoolConfig {
	mysql?: mysql.IMySql;
}

export default class MySQLPooled extends MySQL {

	private _pool: any;

	constructor (options: IPoolConfig) {

		super(options);

		const context = options.mysql ? options.mysql : mysql;
		this._pool = context.createPool(options);
		BBPromise.promisifyAll(this._pool);

	}

	public async withTransaction (delegate: (connection: MySQLConnection) => Promise<void> | void): Promise<void> {

		const connection = await this._pool.getConnectionAsync();
		await MySQLConnection.withTransaction(delegate, {connection});
		connection.release();

	}

	public async tables (db: string): Promise<string[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.tables(connection, db);
		connection.release();
		return result;

	}

	public format (query: string, args: Array<string | number> = []): string {

		return mysql.format(query, args);

	}

	public async query (query: string, queryArgs: any[] = []): Promise<any[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.query(connection, query, queryArgs);
		connection.release();
		return result;

	}

	public async queryAll (db: string, table: string): Promise<any[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.queryAll(connection, db, table);
		connection.release();
		return result;

	}

	public async queryStream (
		query: string,
		delegate: (record) => Promise<void> | void,
		queryArgs: Array<number | string> = []): Promise<void> {

		const connection = await this._pool.getConnectionAsync();
		await actions.queryStream(connection, query, delegate, queryArgs);
		connection.release();

	}

	public async count (db: string, table: string): Promise<number> {

		const connection = await this._pool.getConnectionAsync();
		const result = await actions.count(connection, db, table);
		connection.release();
		return result;

	}

	public async writeRecord (db: string, table: string, record: any): Promise<void> {

		const connection = await this._pool.getConnectionAsync();
		await actions.writeRecord(connection, db, table, record);
		connection.release();

	}

	public async dispose (): Promise<void> {

		await super.dispose();
		await this._pool.endAsync();

	}

}
