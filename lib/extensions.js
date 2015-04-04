'use strict';

var logger = require('aeg-logger');
var mysql = require('mysql');
var async = require('async');
var _ = require('underscore');

var self = {
	query: function (pool, queryInput, args, selectCallback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = mysql.format(queryInput, args);

					connection.query(query, function (err, results) {
						connection.release();

						if (err) {
							logger.error(query);
						}

						callback(err, results, query);
					});
				}
			],
			function (err, results, query) {
				selectCallback(err, results, query);
			});
	},
	count: function (pool, db, tableName, countCallback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = mysql.format('SELECT COUNT (1) AS c FROM ??.??', [db, tableName]);
					connection.query(query, function (err, results) {
						connection.release();

						if (err) {
							logger.error(query);
						}

						callback(err, results[0].c);
					});
				}
			],
			function (err, results) {
				countCallback(err, results);
			});
	},
	writeRecord: function (pool, db, tableName, record, writeRecordCallback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = 'INSERT IGNORE INTO ??.?? SET ?';
					connection.query(query, [db, tableName, record], function (err, results) {
						connection.release();
						callback(err, results);
					});
				}
			],
			function (err, results) {
				writeRecordCallback(err, results);
			});
	},
	queryAll: function (pool, db, tableName, transferDataCallback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var query = mysql.format('SELECT * FROM ??.??', [db, tableName]);
					connection.query(query, function (err, results) {
						connection.release();
						callback(err, results, query);
					});
				}
			],
			function (err, results, query) {
				transferDataCallback(err, results, query);
			});
	},
	getTableDdl: function (pool, db, tableName, ddlCallback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					connection.query('SHOW CREATE TABLE ??.??', [db, tableName], function (err, results) {
						connection.release();
						//replace formatting
						var transformDdl = results[0]['Create Table'].replace(/\n/g, '');
						//remove FK's
						transformDdl = transformDdl.replace(/,\s*CONSTRAINT.*REFERENCES.*`\)/, '');
						//change storage engine
						// transformDdl = transformDdl.replace(/ENGINE=[^\s]*/, 'ENGINE=FEDERATED');
						//add connection
						// transformDdl += ' CONNECTION=\'mysql://{0}:{1}@{2}:{3}/{4}/{5}\''
						// 	.format(pool.config.connectionConfig.user,
						// 		pool.config.connectionConfig.password,
						// 		pool.config.connectionConfig.host,
						// 		pool.config.connectionConfig.port,
						// 		db,
						// 		tableName);
						callback(err, {
							table: tableName,
							ddl: transformDdl
						});
					});
				}
			],
			function (err, result) {
				ddlCallback(err, result);
			});
	},
	getTable: function (pool, db, tableName, tableCallback) {
		async.waterfall([
				function (callback) {
					var ddlResult = [];

					self.getTableDdl(pool, db, tableName, function (err, ddl) {
						ddlResult.push(ddl);
						callback(err, ddlResult);
					});
				}
			],
			function (err, result) {
				tableCallback(err, result);
			});
	},
	getTables: function (pool, db, tablesCallback) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					connection.query('SHOW TABLES IN ??', [db], function (err, results) {
						connection.release();
						callback(err, _.map(results, function (result) {
							return _.values(result)[0];
						}));
					});
				},
				function (tableNames, callback) {

					var ddlResult = [];

					async.each(
						tableNames,
						function (tableName, callback) {
							self.getTableDdl(pool, db, tableName, function (err, ddl) {
								ddlResult.push(ddl);
								callback(err);
							});
						},
						function (err) {
							callback(err, ddlResult);
						});
				}
			],
			function (err, result) {
				tablesCallback(err, result);
			});
	},
	tableExists: function (pool, db, tableName, completionHandler) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					connection.query('SHOW TABLES IN ?? LIKE ?', [db, tableName], function (err, results) {
						connection.release();
						callback(err, results.length > 0);
					});
				}
			],
			function (err, result) {
				completionHandler(err, result);
			});
	},
	createTable: function (pool, tableName, ddl, completionHandler) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					connection.query(ddl, function (err) {
						connection.release();

						if (!err) {
							logger.info('table created', {tableName: tableName});
						} else {
							logger.error(ddl);
						}
						callback(err);
					});
				}
			],
			function (err) {
				completionHandler(err);
			});
	},
	writeTables: function (pool, db, tableNames, completionHandler) {
		var tableNameArr = _.map(tableNames, function (table) {
			return table.table;
		});

		tableNameArr = _.filter(tableNameArr, function(tableName) {
			return tableName.indexOf('h_') === 0 || tableName.indexOf('s_') === 0;
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
									logger.info('create table', {tableName: tableName});
									var ddl = _.find(tableNames, function (table) {
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
				completionHandler(err);
			});
	},
	callProcedure: function (pool, db, procedure, args, completionHandler) {
		async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						callback(err, connection);
					});
				},
				function (connection, callback) {
					var sql = '';

					if (args && args.length !== 0) {
						sql = mysql.format('CALL ??.??(?)', [db, procedure, args.join(',')]);
					} else {
						sql = mysql.format('CALL ??.??()', [db, procedure]);
					}

					connection.query(sql, function (err) {
						connection.release();

						if (err) {
							logger.error(sql);
						}

						callback(err);
					});
				}
			],
			function (err) {
				completionHandler(err);
			});
	}
};

module.exports = self;