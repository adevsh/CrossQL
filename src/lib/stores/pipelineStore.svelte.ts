import { newId } from '$lib/utils';
import type { NodeType, PipelineEdge } from '$lib/types';

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
            host: '', port: 5432,
            user: '', password: '',
            database: '', query: ''
          }
        },
        position: { x: 120, y: position.y }
      }),
      mysql: () => ({
        id, type,
        data: {
          config: {
            host: '', port: 3306,
            user: '', password: '',
            database: '', query: ''
          }
        },
        position: { x: 120, y: position.y }
      }),
      mongodb: () => ({
        id, type,
        data: {
          config: {
            uri: '',
            database: '', collection: '',
            filter: '{}', projection: '{}', flatten_depth: 1
          }
        },
        position: { x: 120, y: position.y }
      }),
      cassandra: () => ({
        id, type,
        data: {
          config: {
            contact_points: '',
            keyspace: '', query: ''
          }
        },
        position: { x: 120, y: position.y }
      }),
      file: () => ({
        id, type,
        data: { config: { path: '' } },
        position: { x: 120, y: position.y }
      }),
      csv_source: () => ({
        id, type,
        data: { config: { path: '' } },
        position: { x: 120, y: position.y }
      }),
      parquet_source: () => ({
        id, type,
        data: { config: { path: '' } },
        position: { x: 120, y: position.y }
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
