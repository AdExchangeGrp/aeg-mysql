import mysql from 'mysql';
import Promise from 'bluebird';
import MySQL from './mysql';
import MySQLConnection from './mysql-connection';
import actions from './actions';

/**
 * Manages MySQL
 */
class MySQLPooled extends MySQL {

	/**
	 * Constructor
	 * @param {Object} [options]
	 */
	constructor (options = {}) {

		super(options);

		this._pool = mysql.createPool(options);
		Promise.promisifyAll(this._pool);

	}

	/**
	 * Perform queries within a transaction
	 * @param {function} delegate
	 */
	async withTransaction (delegate) {

		const connection = await this._pool.getConnectionAsync();
		await MySQLConnection.withTransaction(delegate, {connection});
		connection.release();

	}

	/**
	 * Get the tables in the db
	 * @param {string} db
	 */
	async tables (db) {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.tables(connection, db);
		connection.release();
		return result;

	}

	/**
	 * Query
	 * @param {string} query
	 * @param {Object[]} queryArgs
	 * @return {Object[]}
	 */
	async query (query, queryArgs = []) {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.query(connection, query, queryArgs);
		connection.release();
		return result;

	}

	/**
	 * Query all the records
	 * @param {string} db
	 * @param {string} table
	 */
	async queryAll (db, table) {

		const connection = await this._pool.getConnectionAsync();
		const result = actions.queryAll(connection, db, table);
		connection.release();
		return result;

	}

	/**
	 * Query
	 * @param {string} query
	 * @param {function} delegate
	 * @param {Object} [queryArgs]
	 */
	async queryStream (query, delegate, queryArgs = []) {

		const connection = await this._pool.getConnectionAsync();
		await actions.queryStream(connection, query, delegate, queryArgs);
		connection.release();

	}

	/**
	 * Count
	 * @param {string} db
	 * @param {string} table
	 * @return {number}
	 */
	async count (db, table) {

		const connection = await this._pool.getConnectionAsync();
		const result = await actions.count(connection, db, table);
		connection.release();
		return result;

	}

	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 * @param {string} db
	 * @param {string} table
	 * @param {Object} record
	 */
	async writeRecord (db, table, record) {

		const connection = await this._pool.getConnectionAsync();
		await actions.writeRecord(connection, db, table, record);
		connection.release();

	}

	/**
	 * Dispose the connection when not using a pool
	 */
	async dispose () {

		await super.dispose();
		await this._pool.endAsync();

	}

}

export default MySQLPooled;
