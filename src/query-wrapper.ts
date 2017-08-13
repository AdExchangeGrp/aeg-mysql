import { IConnection, IConnectionConfig } from 'mysql';
import { Segment, SegmentSqlData, SubSegment } from '@adexchange/aeg-xray';

export default async (
	connection: IConnection,
	query: string,
	options: { emitProgress?: boolean, segment?: Segment | undefined } = {}): Promise<any> => {

	if (options.segment) {

		const sub = openSubSegment(connection.config, query, options.segment, options);

		return new Promise((resolve, reject) => {

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

function openSubSegment (
	config: IConnectionConfig, query: string,
	segment: Segment,
	options: { emitProgress?: boolean } = {}): SubSegment {

	const sub = segment.addSubSegment(config.database + '@' + config.host, options);
	sub.addSqlData = new SegmentSqlData(config.user, config.host + ':' + config.port + '/' + config.database, {query});
	return sub;

}
