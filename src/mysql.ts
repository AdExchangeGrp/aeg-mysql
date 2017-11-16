import { Base } from '@adexchange/aeg-common';
import { IMySqlQueryable, IQueryOptions } from './types';
import MySQLConnection from './mysql-connection';

export abstract class MySQL extends Base implements IMySqlQueryable {

	private _isDisposed: boolean;

	constructor (options = {}) {

		super(options);

		this._isDisposed = false;

	}

	get disposed (): boolean {

		return this._isDisposed;

	}

	public async dispose (): Promise<void> {

		this._isDisposed = true;

	}

	public abstract format (query: string, args: Array<string | number>): string;

	public abstract query (query: string, options?: IQueryOptions): Promise<any[]>;

	public abstract writeRecord (db: string, table: string, record: any): Promise<void>;

	public abstract queryWithArgs (
		query: string,
		args: any[],
		options?: IQueryOptions): Promise<any[]>;

	public abstract withTransaction (
		delegate: (connection: MySQLConnection) => Promise<void> | void,
		options?: IQueryOptions): Promise<void>;

}
