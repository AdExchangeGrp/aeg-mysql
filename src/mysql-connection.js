import mysql from 'mysql';
import Promise from 'bluebird';
import MySQL from './mysql';
import actions from './actions';

/**
 * Manages MySQL
 */
class MySQLConnection extends MySQL {

	/**
	 * Constructor
	 * @param {{connection: Object}} [options]
	 */
	constructor (options = {}) {

		super(options);

		if (options.connection) {

			this._connection = options.connection;

		} else {

			this._connection = mysql.createConnection(options);

		}

	}

	/**
	 * Begin a transaction
	 */
	async begin () {

		return actions.begin(this._connection);

	}

	/**
	 * Commit a transaction
	 */
	async commit () {

		return actions.commit(this._connection);

	}

	/**
	 * Rollback a transaction
	 */
	async rollback () {

		return actions.rollback(this._connection);

	}

	/**
	 * Format a query using the underlying connection
	 * @param {string} query
	 * @param {Object[]} [args]
	 */
	format (query, args) {

		return this._connection.format(query, args);

	}

	/**
	 * Perform queries on a connection
	 * @param {function} delegate
	 * @param {{noAutoCommit: boolean}} [options]
	 */
	static async withConnection (delegate, options = {}) {

		// noinspection JSCheckFunctionSignatures
		const mysqlConnection = new MySQLConnection(options);

		if (options.noAutoCommit) {

			await mysqlConnection.query('SET autocommit=0');

		}

		const result = await Promise.resolve(delegate(mysqlConnection));

		if (options.noAutoCommit) {

			await mysqlConnection.query('COMMIT');

		}

		await mysqlConnection.dispose();

		return result;

	}

	/**
	 * Perform queries within a transaction
	 * @param {function} delegate
	 * @param {Object} [options]
	 */
	static async withTransaction (delegate, options = {}) {

		const mysqlConnection = new MySQLConnection(options);

		await mysqlConnection.begin();

		try {

			await Promise.resolve(delegate(mysqlConnection));
			await mysqlConnection.commit();

		} catch (ex) {

			await mysqlConnection.rollback();
			throw ex;

		} finally {

			// do not close the connection if its not managed here
			if (!options.connection) {

				await mysqlConnection.dispose();

			}

		}

	}

	/**
	 * Get the tables in the db
	 * @param {string} db
	 */
	async tables (db) {

		return actions.tables(this._connection, db);

	}

	/**
	 * Query
	 * @param {string} query
	 * @param {Object[]} queryArgs
	 * @return {Object[]}
	 */
	async query (query, queryArgs = []) {

		try {

			return actions.query(this._connection, query, queryArgs);

		} catch (ex) {

			this.error('query', {err: ex});
			throw ex;

		}

	}

	/**
	 * Query all the records
	 * @param {string} db
	 * @param {string} table
	 */
	async queryAll (db, table) {

		return actions.queryAll(this._connection, db, table);

	}

	/**
	 * Query
	 * @param {string} query
	 * @param {function} delegate
	 * @param {Object} [queryArgs]
	 */
	async queryStream (query, delegate, queryArgs = []) {

		return actions.queryStream(this._connection, query, delegate, queryArgs);

	}

	/**
	 * Count
	 * @param {string} db
	 * @param {string} table
	 * @return {number}
	 */
	async count (db, table) {

		return actions.count(this._connection, db, table);

	}

	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 */
	async writeRecord (db, table, record) {

		return actions.writeRecord(this._connection, db, table, record);

	}

	/**
	 * Dispose the connection
	 */
	async dispose () {

		await super.dispose();
		await actions.dispose(this._connection);

	}

}

export default MySQLConnection;
