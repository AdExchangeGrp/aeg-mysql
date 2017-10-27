import { Segment } from '@push_innovation/aeg-xray';
import MySQLConnection from './mysql-connection';

export interface IQueryOptions {
	segment?: Segment;
	emitProgress?: boolean;
}

export interface IMySqlQueryable {
	format (query: string, args: Array<string | number>): string;
	query (query: string, options?: IQueryOptions): Promise<any[]>;
	queryWithArgs (
		query: string,
		args: any[],
		options?: IQueryOptions): Promise<any[]>;
	withTransaction (
		delegate: (connection: MySQLConnection) => Promise<void> | void,
		options?: IQueryOptions): Promise<void>;
}
