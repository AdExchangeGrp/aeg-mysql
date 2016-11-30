import mysql from 'mysql';
import Promise from 'bluebird';
import terminus from 'terminus';
import _ from 'lodash';

/**
 * MySQL actions
 */
export default {

	/**
	 * Begin a transaction
	 * @param {Object} connection
	 */
	async begin (connection) {

		const begin = Promise.promisify(connection.beginTransaction, {context: connection});
		await begin();

	},

	/**
	 * Commit a transaction
	 * @param {Object} connection
	 */
	async commit (connection) {

		const commit = Promise.promisify(connection.commit, {context: connection});
		await commit();

	},

	/**
	 * Rollback a transaction
	 * @param {Object} connection
	 */
	async rollback (connection) {

		const rollback = Promise.promisify(connection.rollback, {context: connection});
		await rollback();

	},

	/**
	 * End a connection
	 * @param {Object} connection
	 */
	async end (connection) {

		const end = Promise.promisify(connection.end, {context: connection});
		await end();

	},

	/**
	 * Get the tables in the db
	 * @param {Object} connection
	 * @param {string} db
	 */
	async tables (connection, db) {

		const result = await this.query(connection, 'SHOW FULL TABLES IN ?? WHERE Table_Type = \'BASE TABLE\'', [db]);
		return _.map(result, (result) => {

			return _.values(result)[0];

		});

	},

	/**
	 * Query
	 * @param {Object} connection
	 * @param {string} query
	 * @param {Object[]} queryArgs
	 * @return {Object[]}
	 */
	async query (connection, query, queryArgs = []) {

		const queryAsync = Promise.promisify(connection.query, {context: connection});
		return queryAsync(connection.format(query, queryArgs));

	},

	/**
	 * Query all the records
	 * @param {Object} connection
	 * @param {string} db
	 * @param {string} table
	 */
	async queryAll (connection, db, table) {

		return this.query(connection, 'SELECT * FROM ??.??', [db, table]);

	},

	/**
	 * Query
	 * @param {Object} connection
	 * @param {string} query
	 * @param {function} delegate
	 * @param {Object} [queryArgs]
	 */
	async queryStream (connection, query, delegate, queryArgs = []) {

		return new Promise((resolve, reject) => {

			let streamErr;

			connection.query(connection.format(query, queryArgs))
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

					if (streamErr) {

						reject(streamErr);

					} else {

						resolve();

					}

				});

		});

	},

	/**
	 * Count
	 * @param {Object} connection
	 * @param {string} db
	 * @param {string} table
	 * @return {number}
	 */
	async count (connection, db, table) {

		const result = await this.query(connection, 'SELECT COUNT (1) AS c FROM ??.??', [db, table]);
		return result[0].c;

	},

	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 * @param {Object} connection
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 */
	async writeRecord (connection, db, table, record) {

		return _writeRecordInternal(connection, db, table, record, true);

	},

	/**
	 * Saves by upsert a record using date-times with the client locale
	 * @param {Object} connection
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 */
	async writeRecordNoLocale (connection, db, table, record) {

		return _writeRecordInternal(connection, db, table, record, false);

	},

	/**
	 * Perform queries within a transaction
	 * @param {Object} connection
	 * @param {function} delegate
	 */
	async withTransaction (connection, delegate) {

		await this.begin(connection);

		try {

			await delegate(connection);
			await this.commit(connection);

		} catch (ex) {

			await this.rollback(connection);
			throw ex;

		}

	}

};

/**
 * Saves by upsert a record
 * @param {Object} connection
 * @param {string} db
 * @param {string} table
 * @param {Object} record
 * @param {Boolean} useConnectionTz
 * @private
 */
async function _writeRecordInternal (connection, db, table, record, useConnectionTz) {

	// noinspection JSCheckFunctionSignatures
	const query = useConnectionTz
		? connection.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record])
		: mysql.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, table, record, record]);
	const queryAsync = Promise.promisify(connection.query, {context: connection});
	return queryAsync(query);

}
