import { Base } from '@adexchange/aeg-common';
import { IMySqlQueryable } from './types';

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

	public abstract query (query: string, queryArgs?: any[]): Promise<any[]>;

}
