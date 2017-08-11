import { Segment } from 'aws-xray-sdk';

export interface IQueryOptions {
	segment?: Segment;
}

export interface IMySqlQueryable {
	format (query: string, args: Array<string | number>): string;
	query (query: string, options?: IQueryOptions): Promise<any[]>;
	queryWithArgs (
		query: string,
		args: Array<string | number | null | undefined>,
		options?: IQueryOptions): Promise<any[]>;
}
