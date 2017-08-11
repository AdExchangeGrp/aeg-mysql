import { Base } from '@adexchange/aeg-common';
import { IMySqlQueryable, IQueryOptions } from './types';

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

	public abstract queryWithArgs (
		query: string,
		args: Array<string | number | null | undefined>,
		options?: IQueryOptions): Promise<any[]>;

}
