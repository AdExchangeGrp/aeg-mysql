import { Segment } from '@adexchange/aeg-xray';
import MySQLConnection from './mysql-connection';

export interface IQueryOptions {
	segment?: Segment;
	emitProgress?: boolean;
}

export interface IMySqlQueryable {
	format (query: string, args: Array<string | number>): string;
	query (query: string, options?: IQueryOptions): Promise<any[]>;
	writeRecord (db: string, table: string, record: any): Promise<void>;
	queryWithArgs (
		query: string,
		args: any[],
		options?: IQueryOptions): Promise<any[]>;
	withTransaction (
		delegate: (connection: MySQLConnection) => Promise<void> | void,
		options?: IQueryOptions): Promise<void>;
}
