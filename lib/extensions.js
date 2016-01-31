'use strict';

var mysql = require('mysql');
var async = require('async');
var _ = require('underscore');
var terminus = require('terminus');

//noinspection JSUnusedGlobalSymbols
var self = {

	/**
	 * Wrapper for creating a pool
	 * @param {Object} options
	 */
	createPool: function (options) {
		return mysql.createPool(options);
	},

	/**
	 * Query mysql
	 * @param {object} pool
	 * @param {string} queryInput - Template string
	 * @param {Array} args
	 * @param {function} callback
	 */
	query: function (pool, queryInput, args, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = connection.format(queryInput, args);

					connection.query(query, function (err, results) {
						connection.release();
						callback(err, results, query);
					});
				}
			],
			function (err, results, query) {
				callback(err, results, query);
			});
	},
	/**
	 * Query mysql using a stream
	 * @param {object} pool
	 * @param {string} queryInput - Template string
	 * @param {Array} args
	 * @param {function} recordCallback - Called on each record in the stream
	 * @param {function} finalCallback
	 */
	queryStream: function (pool, queryInput, args, recordCallback, finalCallback) {
		async.waterfall([
			function (callback) {
				pool.getConnection(function (err, connection) {
					callback(err, connection);
				});
			},
			function (connection, callback) {
				var errorState;
				var query = connection.format(queryInput, args);
				connection.query(query).stream({highWaterMark: 5})
					.on('error', function (err) {
						errorState = err;
					})
					.pipe(terminus({objectMode: true}, processRecord))
					.on('error', function (err) {
						errorState = err;
					})
					.on('finish', function () {
						connection.release();
						callback(errorState);
					});

				//noinspection JSUnusedLocalSymbols
				function processRecord(row, encoding, callback) {
					recordCallback(row, callback);
				}
			}
		], finalCallback);
	},
	/**
	 * Gets a record count for a table
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} tableName
	 * @param {function} callback
	 */
	count: function (pool, db, tableName, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = connection.format('SELECT COUNT (1) AS c FROM ??.??', [db, tableName]);
					connection.query(query, function (err, results) {
						connection.release();
						//noinspection JSUnresolvedVariable
						callback(err, results[0].c);
					});
				}
			],
			function (err, results) {
				callback(err, results);
			});
	},
	/**
	 * Saves by upsert a record converting datetimes to the locale set on the timezone of the server
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} tableName
	 * @param {Object} record
	 * @param {function} callback
	 */
	writeRecord: function (pool, db, tableName, record, callback) {
		this._writeRecordInternal(pool, db, tableName, record, true, callback);
	},
	/**
	 * Saves by upsert a record using datetimes with the client locale
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} tableName
	 * @param {Object} record
	 * @param {function} callback
	 */
	writeRecordNoLocale: function (pool, db, tableName, record, callback) {
		this._writeRecordInternal(pool, db, tableName, record, false, callback);
	},
	/**
	 * Saves by upsert a record
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} tableName
	 * @param {Object} record
	 * @param {Boolean} usePoolTz
	 * @param {function} callback
	 * @private
	 */
	_writeRecordInternal: function (pool, db, tableName, record, usePoolTz, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {

					var query;
					//noinspection JSCheckFunctionSignatures
					query = usePoolTz ?
						connection.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, tableName, record, record]) :
						mysql.format('INSERT INTO ??.?? SET ? ON DUPLICATE KEY UPDATE ?', [db, tableName, record, record]);

					connection.query(query, function (err, results) {
						connection.release();
						callback(err, results);
					});
				}
			],
			function (err, results) {
				callback(err, results);
			});
	},
	/**
	 * Query all the records
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} tableName
	 * @param {function} callback
	 */
	queryAll: function (pool, db, tableName, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = connection.format('SELECT * FROM ??.??', [db, tableName]);
					connection.query(query, function (err, results) {
						connection.release();
						callback(err, results, query);
					});
				}
			],
			function (err, results, query) {
				callback(err, results, query);
			});
	},
	/**
	 * Get the DDL for a table
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} tableName
	 * @param {function} callback
	 * @private
	 */
	_getTableDdl: function (pool, db, tableName, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = connection.format('SHOW CREATE TABLE ??.??', [db, tableName]);
					connection.query(query, function (err, results) {
						connection.release();
						//replace formatting
						var transformDdl = results[0]['Create Table'].replace(/\n/g, '');
						//remove FK's
						transformDdl = transformDdl.replace(/,\s*CONSTRAINT.*REFERENCES.*`\)/, '');
						callback(err, {
							table: tableName,
							ddl: transformDdl
						});
					});
				}
			],
			function (err, result) {
				callback(err, result);
			});
	},
	/**
	 * Get the DDL for one table
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} tableName
	 * @param {function} callback
	 */
	getTable: function (pool, db, tableName, callback) {
		async.waterfall([
				function (callback) {
					var ddlResult = [];

					self._getTableDdl(pool, db, tableName, function (err, ddl) {
						ddlResult.push(ddl);
						callback(err, ddlResult);
					});
				}
			],
			function (err, result) {
				callback(err, result);
			});
	},
	/**
	 * Get the DDL for all of the tables
	 * @param {object} pool
	 * @param {string} db
	 * @param {boolean} getDDL
	 * @param {function} callback
	 */
	getTables: function (pool, db, getDDL, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = connection.format('SHOW FULL TABLES IN ?? WHERE Table_Type = \'BASE TABLE\'', [db]);
					connection.query(query, function (err, results) {
						connection.release();
						callback(err, _.map(results, function (result) {
							return _.values(result)[0];
						}));
					});
				},
				function (tableNames, callback) {
					if (getDDL) {
						var ddlResult = [];
						async.each(
							tableNames,
							function (tableName, callback) {
								self._getTableDdl(pool, db, tableName, function (err, ddl) {
									ddlResult.push(ddl);
									callback(err);
								});
							},
							function (err) {
								callback(err, ddlResult);
							});
					} else {
						callback(null, _.map(tableNames, function (tableName) {
							return {
								table: tableName
							};
						}));
					}
				}
			],
			function (err, result) {
				callback(err, result);
			});
	},
	/**
	 * Check for the existance of a table
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} tableName
	 * @param {function} callback
	 */
	tableExists: function (pool, db, tableName, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = connection.format('SHOW TABLES IN ?? LIKE ?', [db, tableName]);
					connection.query(query, function (err, results) {
						connection.release();
						callback(err, results.length > 0);
					});
				}
			],
			function (err, result) {
				callback(err, result);
			});
	},
	/**
	 * Create a table
	 * @param {object} pool
	 * @param {string} tableName
	 * @param {string} ddl
	 * @param {function} callback
	 */
	createTable: function (pool, tableName, ddl, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					connection.query(ddl, function (err) {
						connection.release();
						callback(err);
					});
				}
			],
			function (err) {
				callback(err);
			});
	},
	/**
	 * Create many tables
	 * @param {object} pool
	 * @param {string} db
	 * @param {Object[]} tables
	 * @param {function} callback
	 */
	writeTables: function (pool, db, tables, callback) {
		var tableNameArr = _.map(tables, function (table) {
			return table.table;
		});

		async.waterfall([
				function (callback) {
					async.each(tableNameArr,
						function (tableName, callback) {
							self.tableExists(pool, db, tableName, function (err, exists) {

								if (err) {
									callback(err);
								}

								if (!exists) {
									//noinspection JSUnresolvedFunction
									var ddl = _.find(tables, function (table) {
										return table.table === tableName;
									}).ddl;
									self.createTable(pool, tableName, ddl, callback);
								} else {
									callback();
								}
							});
						},
						function (err) {
							callback(err);
						});
				}
			],
			function (err) {
				callback(err);
			});
	},
	/**
	 * Call a stored procedure
	 * @param {object} pool
	 * @param {string} db
	 * @param {string} procedure
	 * @param {Array} args
	 * @param {function} callback
	 */
	callProcedure: function (pool, db, procedure, args, callback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = '';

					if (args && args.length !== 0) {
						query = connection.format('CALL ??.??(?)', [db, procedure, args.join(',')]);
					} else {
						query = connection.format('CALL ??.??()', [db, procedure]);
					}

					connection.query(query, function (err) {
						connection.release();
						callback(err);
					});
				}
			],
			function (err) {
				callback(err);
			});
	}
};

module.exports = self;