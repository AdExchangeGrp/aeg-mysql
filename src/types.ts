export interface IMySqlQueryable {
	format (query: string, args?: Array<string | number>): string;
	query (query: string, queryArgs?: any[]): Promise<any[]>;
}
