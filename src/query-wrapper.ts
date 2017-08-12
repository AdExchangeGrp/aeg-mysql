import * as BBPromise from 'bluebird';
import { IConnection } from 'mysql';
import { Segment } from 'aws-xray-sdk';
import * as SqlData from 'aws-xray-sdk-core/lib/database/sql_data';
import { IPoolConfig } from 'mysql';

export default (
	connection: IConnection,
	query: string,
	segment?: Segment): BBPromise<any> => {

	if (segment) {

		return new Promise((resolve, reject) => {

			connection.query(query, (err, result) => {

				if (err) {

					writeSubSegment(connection.config, query, segment);
					reject(err);

				} else {

					writeSubSegment(connection.config, query, segment);
					resolve(result);

				}

			}, segment);

		});

	} else {

		return new Promise((resolve, reject) => {

			connection.query(query, (err, result) => {

				if (err) {

					reject(err);

				} else {

					resolve(result);

				}

			});

		});

	}

};

function writeSubSegment (
	config: IPoolConfig,
	query: string,
	segment: Segment,
	err?: Error): void {

	const sub = segment.addNewSubsegment(config.database + '@' + config.host);
	sub.addSqlData(new SqlData(null, null, config.user, config.host + ':' + config.port + '/' + config.database, query));
	sub.namespace = 'remote';
	sub.close(err);

}
