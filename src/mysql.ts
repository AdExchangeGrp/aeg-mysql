import { Base } from '@adexchange/aeg-common';

/**
 * MySQL
 */
class MySQL extends Base {

	private _isDisposed: boolean;

	/**
	 * Constructor
	 */
	constructor (options = {}) {

		super(options);

		this._isDisposed = false;

	}

	/**
	 * Is this instance disposed
	 */
	get disposed (): boolean {

		return this._isDisposed;

	}

	/**
	 * Dispose
	 */
	public async dispose (): Promise<void> {

		this._isDisposed = true;

	}

}

export default MySQL;
