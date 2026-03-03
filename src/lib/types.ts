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
  left: string; // Node ID
  right: string; // Node ID
  left_on: string;
  right_on: string;
  how: 'inner' | 'left' | 'outer' | 'cross';
}

export interface ParquetConfig {
  path: string;
  compression: 'snappy' | 'zstd' | 'gzip' | 'lz4' | 'none';
  row_group_size?: number;
}
