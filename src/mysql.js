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
	constructor (options = {}) {

		const params = _.cloneDeep(options);

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
	static async withConnection (delegate, options = {}) {

		const params = _.cloneDeep(options);
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
	 * Perform queries within a transaction
	 * @param {function} delegate
	 */
	async withTransaction (delegate) {

		const connection = await this._resolveConnection();

		const begin = Promise.promisify(connection.beginTransaction, {context: connection});
		await begin();

		try {

			await delegate(connection);
			const commit = Promise.promisify(connection.commit, {context: connection});
			await commit();

		} catch (ex) {

			const rollback = Promise.promisify(connection.rollback, {context: connection});
			await rollback();
			throw ex;

		} finally {

			await this._releaseConnection(connection);

		}

	}

	/**
	 * Get the tables in the db
	 * @param {Object} [options]
	 * @param {string} db
	 */
	async tables (db, options = {}) {

		const result = await this.query('SHOW FULL TABLES IN ?? WHERE Table_Type = \'BASE TABLE\'', [db], options);
		return _.map(result, (result) => {

			return _.values(result)[0];

		});

	}

	/**
	 * Query
	 * @param {string} query
	 * @param {Object[]} queryArgs
	 * @param {Object} [options]
	 * @return {Object[]}
	 */
	async query (query, queryArgs = [], options = {}) {

		const connection = await this._resolveConnection(options);

		try {

			const queryAsync = Promise.promisify(connection.query, {context: connection});
			const result = await queryAsync(connection.format(query, queryArgs));

			await this._releaseConnection(connection);

			return result;

		} catch (ex) {

			this.error('query', {message: 'query error', err: ex});

			if (!options.connection) {

				await this._releaseConnection(connection);

			}

			throw ex;

		}

	}

	/**
	 * Query all the records
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} [options]
	 */
	async queryAll (db, table, options = {}) {

		return this.query('SELECT * FROM ??.??', [db, table], options);

	}

	/**
	 * Query
	 * @param {string} query
	 * @param {function} delegate
	 * @param {Object} [options]
	 */
	async queryStream (query, delegate, options = {}) {

		const self = this;

		const connection = await self._resolveConnection(options);

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

					if (!options.connection) {

						self._releaseConnection(connection).catch((ex) => {

							this.error('queryStream', {message: 'error releasing connection', err: ex});

						});

					}

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
	 * @param {Object} [options]
	 */
	async count (db, table, options = {}) {

		const result = await this.query('SELECT COUNT (1) AS c FROM ??.??', [db, table], options);
		return result[0].c;

	}

	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 * @param {Object} [options]
	 */
	async writeRecord (db, table, record, options = {}) {

		return this._writeRecordInternal(db, table, record, true, options);

	}

	/**
	 * Saves by upsert a record using datetimes with the client locale
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 * @param {Object} [options]
	 */
	async writeRecordNoLocale (db, table, record, options = {}) {

		return this._writeRecordInternal(db, table, record, false, options);

	}

	/**
	 * Dispose the connection when not using a pool
	 */
	async dispose () {

		if (this._connection) {

			const end = Promise.promisify(this._connection.end, {context: this._connection});
			await end();

		} else {

			await this._pool.endAsync();

		}

	}

	/**
	 * Resolve the connection via pool or static connection
	 * @param {Object} [options]
	 * @return {Connection|*}
	 * @private
	 */
	async _resolveConnection (options = {}) {

		if (options.connection) {

			return options.connection;

		}

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
	 * @param {Object} [options]
	 * @private
	 */
	async _writeRecordInternal (db, table, record, usePoolTz, options = {}) {

		const connection = await this._resolveConnection(options);
		// noinspection JSCheckFunctionSignatures
		const query = usePoolTz
			? connection.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record])
			: mysql.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record]);
		const queryAsync = Promise.promisify(connection.query, {context: connection});
		const results = await queryAsync(query);

		if (!options.connection) {

			await this._releaseConnection(connection);

		}

		return results;

	}

}

export default MySQL;
