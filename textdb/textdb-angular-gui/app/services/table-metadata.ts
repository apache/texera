export class TableMetadata {
    tableName: string;
    attributes: Map<string, any>;

    constructor(tableName: string, attributes: Map<string, any>)
    {
        this.tableName = tableName;
        this.attributes = attributes;
    }
}