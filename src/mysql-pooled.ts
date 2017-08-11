import * as mysql from 'mysql';
import * as BBPromise from 'bluebird';
import { MySQL } from './mysql';
import MySQLConnection from './mysql-connection';
import actions from './actions';
import { IPoolConfig as IMySQLPoolConfig } from 'mysql';
import { IQueryOptions } from './types';

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

	public async withTransaction (
		delegate: (connection: MySQLConnection) => Promise<void> | void,
		options: IQueryOptions = {}): Promise<void> {

		const connection = await this._pool.getConnectionAsync();
		await MySQLConnection.withTransaction(delegate, Object.assign({}, {connection}, options));
		connection.release();

	}

	public async tables (db: string, options: IQueryOptions = {}): Promise<string[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.tables(connection, db, options);
		connection.release();
		return result;

	}

	public format (query: string, args: Array<string | number | null | undefined> = []): string {

		return mysql.format(query, args);

	}

	public async query (query: string, options: IQueryOptions = {}): Promise<any[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.query(connection, query, options);
		connection.release();
		return result;

	}

	public async queryWithArgs (
		query: string,
		args: Array<string | number | null | undefined>,
		options: IQueryOptions = {}): Promise<any[]> {

		return this.query(this.format(query, args), options);

	}

	public async queryAll (db: string, table: string, options: IQueryOptions = {}): Promise<any[]> {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.queryAll(connection, db, table, options);
		connection.release();
		return result;

	}

	public async count (db: string, table: string, options: IQueryOptions = {}): Promise<number> {

		const connection = await this._pool.getConnectionAsync();
		const result = await actions.count(connection, db, table, options);
		connection.release();
		return result;

	}

	public async writeRecord (db: string, table: string, record: any, options: IQueryOptions = {}): Promise<void> {

		const connection = await this._pool.getConnectionAsync();
		await actions.writeRecord(connection, db, table, record, options);
		connection.release();

	}

	public async dispose (): Promise<void> {

		await super.dispose();
		await this._pool.endAsync();

	}

}
