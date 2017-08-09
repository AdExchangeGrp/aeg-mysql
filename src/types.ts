export interface IMySqlQueryable {
	query (query: string, queryArgs?: any[]): Promise<any[]>;
}
