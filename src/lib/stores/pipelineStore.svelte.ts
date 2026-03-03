import { newId } from '$lib/utils';

type NodeType =
  | 'postgres'
  | 'mysql'
  | 'mongodb'
  | 'cassandra'
  | 'schema_map'
  | 'join'
  | 'filter'
  | 'select'
  | 'rename'
  | 'cast'
  | 'derived'
  | 'parquet';

function createPipelineStore() {
  let nodes = $state<any[]>([
    {
      id: 'out1',
      type: 'parquet',
      data: { config: { path: '/tmp/test.parquet', compression: 'snappy', row_group_size: 524288 } },
      position: { x: 600, y: 100 },
    }
  ]);

  let edges = $state<any[]>([]);
  let selectedNodeId = $state<string | null>(null);

  function nextPosition() {
    const baseX = 120;
    const baseY = 120;
    const stepY = 140;
    return { x: baseX, y: baseY + nodes.length * stepY };
  }

  function updateNodeData(nodeId: string, nextData: any) {
    nodes = nodes.map((n: any) =>
      n.id === nodeId ? { ...n, data: { ...(n.data ?? {}), ...nextData } } : n
    );
  }

  function setAllNodeRunState(state: 'idle' | 'running' | 'done' | 'error', error?: string) {
    nodes = nodes.map((n: any) => ({
      ...n,
      data: { ...(n.data ?? {}), run_state: state, run_error: error ?? null }
    }));
  }

  function applyNodeStats(nodeStats: any[]) {
    nodes = nodes.map((n: any) => {
      const st = nodeStats.find((x: any) => x.id === n.id);
      if (!st) return n;
      return { ...n, data: { ...(n.data ?? {}), stats: { rows_left: st.rows_left, rows_right: st.rows_right, rows_out: st.rows_out } } };
    });
  }

  function addNode(type: NodeType) {
    const id = newId(type);
    const position = nextPosition();

    const defaults: Record<NodeType, () => any> = {
      postgres: () => ({
        id, type,
        data: {
          config: {
            host: 'postgresql.crossql.orb.local', port: 5432,
            user: 'postgres', password: 'postgres',
            database: 'crossql', query: 'SELECT 1 as id'
          }
        },
        position
      }),
      mysql: () => ({
        id, type,
        data: {
          config: {
            host: 'mysql.crossql.orb.local', port: 3306,
            user: 'crossql', password: 'crossql',
            database: 'crossql', query: 'SELECT 1 as id'
          }
        },
        position
      }),
      mongodb: () => ({
        id, type,
        data: {
          config: {
            uri: 'mongodb://root:root@mongodb.crossql.orb.local:27017',
            database: '', collection: '',
            filter: '{}', projection: '{}', flatten_depth: 1
          }
        },
        position
      }),
      cassandra: () => ({
        id, type,
        data: {
          config: {
            contact_points: 'cassandra.crossql.orb.local:9042',
            keyspace: 'crossql', query: 'SELECT * FROM regions;'
          }
        },
        position
      }),
      schema_map: () => ({
        id, type,
        data: { config: { columns: [] } },
        position: { x: 360, y: position.y }
      }),
      join: () => ({
        id, type,
        data: { config: { how: 'inner', left_on: 'id', right_on: 'id' } },
        position: { x: 470, y: position.y }
      }),
      filter: () => ({
        id, type,
        data: { config: { column: '', op: 'eq', value_type: 'string', value: '' } },
        position: { x: 360, y: position.y }
      }),
      select: () => ({
        id, type,
        data: { config: { columns: [] } },
        position: { x: 360, y: position.y }
      }),
      rename: () => ({
        id, type,
        data: { config: { mappings: [] } },
        position: { x: 360, y: position.y }
      }),
      cast: () => ({
        id, type,
        data: { config: { casts: [] } },
        position: { x: 360, y: position.y }
      }),
      derived: () => ({
        id, type,
        data: { config: { name: 'derived', op: 'upper', left: '', right_kind: 'column', right: '' } },
        position: { x: 360, y: position.y }
      }),
      parquet: () => ({
        id, type,
        data: { config: { path: `/tmp/${id}.parquet`, compression: 'snappy', row_group_size: 524288 } },
        position: { x: 650, y: position.y }
      }),
    };

    nodes = [...nodes, defaults[type]()];
  }

  return {
    get nodes() { return nodes; },
    set nodes(v: any[]) { nodes = v; },
    get edges() { return edges; },
    set edges(v: any[]) { edges = v; },
    get selectedNodeId() { return selectedNodeId; },
    set selectedNodeId(v: string | null) { selectedNodeId = v; },
    addNode,
    updateNodeData,
    setAllNodeRunState,
    applyNodeStats,
  };
}

export const pipelineStore = createPipelineStore();
