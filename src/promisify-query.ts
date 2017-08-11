import * as BBPromise from 'bluebird';
import { IConnection } from 'mysql';
import { Segment } from 'aws-xray-sdk';

export default (connection: IConnection, segment?: Segment): BBPromise<any> => {

	if (segment) {

		return BBPromise.promisify<any>(
			(query, callback) => connection.query(query, callback, segment),
			{context: connection});

	} else {

		return BBPromise.promisify<any>(
			(query, callback) => connection.query(query, callback),
			{context: connection});

	}

};
