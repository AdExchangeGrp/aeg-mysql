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

		super(options);
		this._pool = mysql.createPool(options);
		Promise.promisifyAll(this._pool);

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

		const connection = await this._pool.getConnectionAsync();

		try {

			const queryAsync = Promise.promisify(connection.query, {context: connection});
			const result = await queryAsync(connection.format(query, options));
			connection.release();
			return result;

		} catch (ex) {

			this.error('query', {message: 'query error', err: ex});

			connection.release();

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

		const connection = await this._pool.getConnectionAsync();

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

					connection.release();

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
	 * Saves by upsert a record
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 * @param {Boolean} usePoolTz
	 * @private
	 */
	async _writeRecordInternal (db, table, record, usePoolTz) {

		const connection = await this._pool.getConnectionAsync();
		const queryAsync = Promise.promisify(connection.query, {context: connection});
		// noinspection JSCheckFunctionSignatures
		const query = usePoolTz
			? connection.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record])
			: mysql.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record]);
		const results = queryAsync(query);
		connection.release();
		return results;

	}

}

export default MySQL;
