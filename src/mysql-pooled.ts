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

		return this._withConnection((connection) =>
			MySQLConnection.withTransaction(delegate, Object.assign({}, {connection}, options)));

	}

	public async tables (db: string, options: IQueryOptions = {}): Promise<string[]> {

		return this._withConnection((connection) => actions.tables(connection, db, options));

	}

	public format (query: string, args: any[] = []): string {

		return mysql.format(query, args);

	}

	public async query (query: string, options: IQueryOptions = {}): Promise<any[]> {

		return this._withConnection((connection) => actions.query(connection, query, options));

	}

	public async queryWithArgs (
		query: string,
		args: any[],
		options: IQueryOptions = {}): Promise<any[]> {

		return this.query(this.format(query, args), options);

	}

	public async count (db: string, table: string, options: IQueryOptions = {}): Promise<number> {

		return this._withConnection((connection) => actions.count(connection, db, table, options));

	}

	public async writeRecord (db: string, table: string, record: any, options: IQueryOptions = {}): Promise<void> {

		return this._withConnection((connection) => actions.writeRecord(connection, db, table, record, options));

	}

	public async dispose (): Promise<void> {

		await super.dispose();
		const end = BBPromise.promisify(this._pool.end, {context: this._pool});
		await end();

	}

	private async _getConnection (): Promise<IConnection> {

		const getConnection: any = BBPromise.promisify(this._pool.getConnection, {context: this._pool});
		return getConnection();

	}

	private async _withConnection<T> (delegate: (connection: IConnection) => Promise<T>): Promise<T> {

		const connection = await this._getConnection();
		let result: T;
		try {

			result = await delegate(connection);

		} finally {

			connection.release();

		}

		return result;

	}

}
