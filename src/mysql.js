import mysql from 'mysql';
import Promise from 'bluebird';
import terminus from 'terminus';
import _ from 'lodash';
import { Base } from '@adexchange/aeg-common';

/**
 * Manages MySQL
 */
class MySQL extends Base {

	/**
	 * Constructor
	 * @param {Object} [options]
	 */
	constructor (options) {

		const params = _.cloneDeep(options || {});

		super(params);

		if (params.noPool) {

			this._connection = mysql.createConnection(params);

		} else {

			this._pool = mysql.createPool(params);
			Promise.promisifyAll(this._pool);

		}

	}

	/**
	 * Perform queries on a connection
	 * @param {function} delegate
	 * @param {{noAutoCommit: boolean}} [options]
	 */
	static async withConnection (delegate, options) {

		const params = _.cloneDeep(options || {});
		params.noPool = true;

		const mysql = new MySQL(params);

		if (params.noAutoCommit) {

			await mysql.query('SET autocommit=0');

		}

		await Promise.resolve(delegate(mysql));

		if (params.noAutoCommit) {

			await mysql.query('COMMIT');

		}

		await mysql.dispose();

	}

	/**
	 * Get the tables in the db
	 * @param {string} db
	 */
	async tables (db) {

		const result = await this.query('SHOW FULL TABLES IN ?? WHERE Table_Type = \'BASE TABLE\'', [db]);
		return _.map(result, (result) => {

			return _.values(result)[0];

		});

	}

	/**
	 * Query
	 * @param {string} query
	 * @param {Object} [options]
	 * @return {Object[]}
	 */
	async query (query, options) {

		const connection = await this._resolveConnection();

		try {

			const queryAsync = Promise.promisify(connection.query, {context: connection});
			const result = await queryAsync(connection.format(query, options));

			await this._releaseConnection(connection);

			return result;

		} catch (ex) {

			this.error('query', {message: 'query error', err: ex});

			await this._releaseConnection(connection);

			throw ex;

		}

	}

	/**
	 * Query all the records
	 * @param {string} db
	 * @param {string} table
	 */
	async queryAll (db, table) {

		return this.query('SELECT * FROM ??.??', [db, table]);

	}

	/**
	 * Query
	 * @param {string} query
	 * @param {function} delegate
	 * @param {Object} [options]
	 */
	async queryStream (query, delegate, options) {

		const self = this;

		const connection = await self._resolveConnection();

		return new Promise((resolve, reject) => {

			let streamErr;

			connection.query(connection.format(query, options))
				.stream({highWaterMark: 5})
				.on('error', (err) => {

					streamErr = err;

				})
				.pipe(terminus({objectMode: true}, (record, encoding, callback) => {

					Promise.resolve(delegate(record))
						.then(() => {

							callback();

						})
						.catch((ex) => {

							callback(ex);

						});

				}))
				.on('error', (err) => {

					streamErr = err;

				})
				.on('finish', () => {

					self._releaseConnection(connection).catch((ex) => {

						this.error('queryStream', {message: 'error releasing connection', err: ex});

					});

					if (streamErr) {

						reject(streamErr);

					} else {

						resolve();

					}

				});

		});

	}

	/**
	 * Count
	 * @param {string} db
	 * @param {string} table
	 * @return {number}
	 */
	async count (db, table) {

		const result = await this.query('SELECT COUNT (1) AS c FROM ??.??', [db, table]);
		return result[0].c;

	}

	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 */
	async writeRecord (db, table, record) {

		return this._writeRecordInternal(db, table, record, true);

	}

	/**
	 * Saves by upsert a record using datetimes with the client locale
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 */
	async writeRecordNoLocale (db, table, record) {

		return this._writeRecordInternal(db, table, record, false);

	}

	/**
	 * Dispose the connection when not using a pool
	 */
	async dispose () {

		if (this._connection) {

			const end = Promise.promisify(this._connection.end, {context: this._connection});
			await end();

		}

	}

	/**
	 * Resolve the connection via pool or static connection
	 * @return {Connection|*}
	 * @private
	 */
	async _resolveConnection () {

		return this._connection ? this._connection : await this._pool.getConnectionAsync();

	}

	/**
	 * Release the connection if its pooled
	 * @param {Connection} connection
	 * @private
	 */
	async _releaseConnection (connection) {

		if (this._pool) {

			connection.release();

		}

	}

	/**
	 * Saves by upsert a record
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 * @param {Boolean} usePoolTz
	 * @private
	 */
	async _writeRecordInternal (db, table, record, usePoolTz) {

		const connection = await this._resolveConnection();
		// noinspection JSCheckFunctionSignatures
		const query = usePoolTz
			? connection.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record])
			: mysql.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record]);
		const queryAsync = Promise.promisify(connection.query, {context: connection});
		const results = await queryAsync(query);
		await this._releaseConnection(connection);
		return results;

	}

}

export default MySQL;
