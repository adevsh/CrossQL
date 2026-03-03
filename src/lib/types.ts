export interface Pipeline {
  id: string;
  name: string;
  description?: string;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
}

export type NodeType = 
  | 'postgres' 
  | 'mysql' 
  | 'mongodb' 
  | 'cassandra' 
  | 'join' 
  | 'schema_map' 
  | 'filter' 
  | 'select'
  | 'rename'
  | 'cast'
  | 'derived'
  | 'parquet';

export interface PipelineNode {
  id: string;
  type: NodeType;
  position: { x: number; y: number };
  config: NodeConfig;
}

export interface PipelineEdge {
  id: string;
  source: string;
  target: string;
}

export type NodeConfig = 
  | PostgresConfig 
  | MySQLConfig 
  | MongoConfig 
  | CassandraConfig 
  | JoinConfig 
  | SchemaMapConfig
  | FilterConfig
  | SelectConfig
  | RenameConfig
  | CastConfig
  | DerivedColumnConfig
  | ParquetConfig 
  | Record<string, any>;

export interface PostgresConfig {
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  query?: string;
}

export interface MySQLConfig {
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  query?: string;
}

export interface MongoConfig {
  uri?: string;
  database?: string;
  collection?: string;
  filter?: string; // JSON string
  projection?: string; // JSON string
  flatten_depth?: number;
}

export interface CassandraConfig {
  contact_points?: string;
  keyspace?: string;
  query?: string;
}

export interface JoinConfig {
  left?: string; // Node ID
  right?: string; // Node ID
  left_on: string;
  right_on: string;
  how: 'inner' | 'left' | 'outer';
}

export interface SchemaMapConfig {
  columns: Array<{
    source: string;
    rename?: string;
    cast?: '' | 'Int64' | 'Float64' | 'Boolean' | 'Utf8' | 'Datetime';
    null_mode?: 'keep' | 'drop_row' | 'fill_default' | 'error';
    fill_value?: string;
  }>;
}

export interface FilterConfig {
  column: string;
  op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'is_null' | 'is_not_null';
  value_type: 'string' | 'number' | 'boolean';
  value: string;
}

export interface SelectConfig {
  columns: string[];
}

export interface RenameConfig {
  mappings: Array<{ from: string; to: string }>;
}

export interface CastConfig {
  casts: Array<{ column: string; dtype: 'Int64' | 'Float64' | 'Boolean' | 'Utf8' | 'Datetime' }>;
}

export interface DerivedColumnConfig {
  name: string;
  op: 'upper' | 'lower' | 'add' | 'concat';
  left: string;
  right_kind: 'column' | 'literal';
  right: string;
}

export interface ParquetConfig {
  path: string;
  compression: 'snappy' | 'zstd' | 'gzip' | 'lz4' | 'none';
  row_group_size?: number;
}
