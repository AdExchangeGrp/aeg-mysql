import { Base } from '@adexchange/aeg-common';

/**
 * MySQL
 */
class MySQL extends Base {

	/**
	 * Constructor
	 * @param {Object} [options]
	 */
	constructor (options = {}) {

		super(options);

		this._isDisposed = false;

	}

	/**
	 * Is this instance disposed
	 * @returns {boolean}
	 */
	get disposed () {

		return this._isDisposed;

	}

	/**
	 * Dispose
	 */
	async dispose () {

		this._isDisposed = true;

	}

}

export default MySQL;
