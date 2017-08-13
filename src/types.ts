import { Segment } from '@adexchange/aeg-xray';

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
}
