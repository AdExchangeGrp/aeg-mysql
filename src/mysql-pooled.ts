import * as mysql from 'mysql';
import { MySQL } from './mysql';
import MySQLConnection from './mysql-connection';
import actions from './actions';
import { IConnection, IPoolConfig } from 'mysql';
import { IQueryOptions } from './types';
import * as BBPromise from 'bluebird';

export default class MySQLPooled extends MySQL {

	private _pool: any;

	constructor (options: IPoolConfig) {

		super(options);

		this._pool = mysql.createPool(options);

	}

	public async withTransaction (
		delegate: (connection: MySQLConnection) => Promise<void> | void,
		options: IQueryOptions = {}): Promise<void> {

		const connection = await this._getConnection();
		await MySQLConnection.withTransaction(delegate, Object.assign({}, {connection}, options));
		connection.release();

	}

	public async tables (db: string, options: IQueryOptions = {}): Promise<string[]> {

		const connection = await this._getConnection();
		const result = actions.tables(connection, db, options);
		connection.release();
		return result;

	}

	public format (query: string, args: any[] = []): string {

		return mysql.format(query, args);

	}

	public async query (query: string, options: IQueryOptions = {}): Promise<any[]> {

		const connection = await this._getConnection();
		const result = actions.query(connection, query, options);
		connection.release();
		return result;

	}

	public async queryWithArgs (
		query: string,
		args: any[],
		options: IQueryOptions = {}): Promise<any[]> {

		return this.query(this.format(query, args), options);

	}

	public async count (db: string, table: string, options: IQueryOptions = {}): Promise<number> {

		const connection = await this._getConnection();
		const result = await actions.count(connection, db, table, options);
		connection.release();
		return result;

	}

	public async writeRecord (db: string, table: string, record: any, options: IQueryOptions = {}): Promise<void> {

		const connection = await this._getConnection();
		await actions.writeRecord(connection, db, table, record, options);
		connection.release();

	}

	public async dispose (): Promise<void> {

		await super.dispose();
		const end = BBPromise.promisify(this._pool.end, {context: this._pool});
		await end();

	}

	private async _getConnection (): Promise<IConnection> {

		const getConnection = BBPromise.promisify(this._pool.getConnection, {context: this._pool});
		return getConnection();

	}

}
