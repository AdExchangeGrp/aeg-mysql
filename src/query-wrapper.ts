import { IConnection, IPoolConfig } from 'mysql';
import { Segment, SegmentSqlData } from '@adexchange/aeg-xray';

export default (
	connection: IConnection,
	query: string,
	segment?: Segment): Promise<any> => {

	if (segment) {

		return new Promise((resolve, reject) => {

			const sub = openSubSegment(connection.config, query, segment);

			connection.query(query, (err, result) => {

				if (err) {

					sub.close(err);
					reject(err);

				} else {

					sub.close();
					resolve(result);

				}

			});

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

function openSubSegment (config: IPoolConfig, query: string, segment: Segment): Segment {
	const sub = new Segment(config.database + '@' + config.host);
	segment.addSubSegment(sub);
	const sql = new SegmentSqlData(config.user, config.host + ':' + config.port + '/' + config.database, {query});
	sub.addSqlData(sql);
	return sub;
}
