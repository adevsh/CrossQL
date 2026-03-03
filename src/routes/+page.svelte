<script lang="ts">
  import PipelineCanvas from '$lib/components/PipelineCanvas.svelte';
  import TypeBadge from '$lib/components/TypeBadge.svelte';
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from '@tauri-apps/api/event';
  import { onMount } from 'svelte';

  let invokeResult = $state("Idle");

  let runState = $state<'idle' | 'running' | 'success' | 'error'>('idle');
  let runRowCount = $state<number | null>(null);
  let runFileSizeBytes = $state<number | null>(null);
  let runOutputPath = $state<string | null>(null);
  let runId = $state<string | null>(null);
  let runLogs = $state<Array<{ ts: number; message: string }>>([]);

  type PipelineEvent = {
    run_id: string;
    kind: string;
    node_id?: string | null;
    state?: string | null;
    message?: string | null;
    result?: any | null;
  };

  function appendLog(message: string) {
    runLogs = [...runLogs, { ts: Date.now(), message }].slice(-300);
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

  onMount(() => {
    const unlistenPromise = listen<PipelineEvent>('pipeline_event', (event) => {
      const p = event.payload;
      if (!p) return;

      if (p.kind === 'run_started') {
        runId = p.run_id;
        runState = 'running';
        invokeResult = 'Running…';
        runRowCount = null;
        runFileSizeBytes = null;
        runOutputPath = null;
        runLogs = [];
        setAllNodeRunState('running');
        appendLog('Run started');
        return;
      }

      if (runId && p.run_id !== runId) return;

      if (p.kind === 'node_state' && p.node_id) {
        updateNodeData(p.node_id, { run_state: p.state ?? 'idle', run_error: p.message ?? null });
        if (p.state) appendLog(`${p.node_id}: ${p.state}`);
        return;
      }

      if (p.kind === 'run_finished' && p.result) {
        runState = 'success';
        if (typeof p.result?.row_count === 'number' && typeof p.result?.file_size_bytes === 'number' && typeof p.result?.path === 'string') {
          const rowCount = p.result.row_count as number;
          const fileSizeBytes = p.result.file_size_bytes as number;
          const outPath = p.result.path as string;
          runRowCount = rowCount;
          runFileSizeBytes = fileSizeBytes;
          runOutputPath = outPath;
          invokeResult = `Success: ${rowCount} rows → ${outPath} (${formatBytes(fileSizeBytes)})`;
        } else {
          invokeResult = 'Success';
        }
        if (Array.isArray(p.result.node_stats)) {
          applyNodeStats(p.result.node_stats);
        }
        setAllNodeRunState('done');
        appendLog('Run finished');
        runId = null;
        return;
      }

      if (p.kind === 'run_cancelled') {
        runState = 'idle';
        invokeResult = 'Cancelled';
        setAllNodeRunState('idle');
        appendLog('Run cancelled');
        runId = null;
        return;
      }

      if (p.kind === 'run_error') {
        runState = 'error';
        invokeResult = `Error: ${p.message ?? 'Unknown error'}`;
        setAllNodeRunState('error', p.message ?? 'Unknown error');
        appendLog(`Error: ${p.message ?? 'Unknown error'}`);
        runId = null;
      }
    });

    return () => {
      unlistenPromise.then((u) => u());
    };
  });

  let selectedNodeId = $state<string | null>(null);
  let schemaState = $state<'idle' | 'loading' | 'ready' | 'error'>('idle');
  let schemaError = $state<string | null>(null);
  let schemaFields = $state<Array<{ name: string; dtype: string }>>([]);

  let previewState = $state<'idle' | 'loading' | 'ready' | 'error'>('idle');
  let previewError = $state<string | null>(null);
  let previewColumns = $state<string[]>([]);
  let previewRows = $state<any[]>([]);

  let usageState = $state<'idle' | 'loading' | 'ready' | 'error'>('idle');
  let usageError = $state<string | null>(null);
  let usageCpuPercent = $state<number | null>(null);
  let usageMemoryBytes = $state<number | null>(null);

  function applySchemaMap(fields: Array<{ name: string; dtype: string }>, cfg: any) {
    const map = new Map<string, string>(fields.map((f) => [f.name, f.dtype]));
    const cols: any[] = cfg?.columns ?? [];
    for (const c of cols) {
      const source = typeof c?.source === 'string' ? c.source.trim() : '';
      if (!source) continue;
      const sourceDtype = map.get(source);
      if (!sourceDtype) continue;
      const rename = typeof c?.rename === 'string' ? c.rename.trim() : '';
      const cast = typeof c?.cast === 'string' ? c.cast.trim() : '';
      const targetName = rename || source;
      const targetDtype = cast ? cast : sourceDtype;
      if (targetName !== source) map.delete(source);
      map.set(targetName, targetDtype);
    }
    return Array.from(map.entries()).map(([name, dtype]) => ({ name, dtype }));
  }

  async function fetchSchemaForNode(nodeId: string, depth = 0): Promise<Array<{ name: string; dtype: string }>> {
    if (depth > 20) throw new Error('Schema resolution exceeded max depth');
    const node = nodes.find((n: any) => n.id === nodeId);
    if (!node) throw new Error('Node not found');

    if (node.type === 'schema_map') {
      const incomingEdges = edges.filter((e: any) => e.target === node.id);
      if (incomingEdges.length !== 1) throw new Error('Schema Map must have exactly one incoming edge');
      const upstreamId = incomingEdges[0].source;
      const upstreamSchema = await fetchSchemaForNode(upstreamId, depth + 1);
      return applySchemaMap(upstreamSchema, node.data?.config);
    }

    if (node.type !== 'postgres' && node.type !== 'mysql' && node.type !== 'mongodb' && node.type !== 'cassandra') {
      throw new Error('Schema preview is only available for source and Schema Map nodes');
    }

    if (!node.data?.config) throw new Error('Missing node config');

    const command =
      node.type === 'postgres'
        ? 'preview_postgres_schema'
        : node.type === 'mysql'
          ? 'preview_mysql_schema'
          : node.type === 'mongodb'
            ? 'preview_mongodb_schema'
            : 'preview_cassandra_schema';
    const result = await invoke(command, { source: node.data.config });
    return (result as any[])?.map((x: any) => ({ name: x.name, dtype: x.dtype })) ?? [];
  }

  function nextPosition() {
    const baseX = 120;
    const baseY = 120;
    const stepY = 140;
    return { x: baseX, y: baseY + nodes.length * stepY };
  }

  function newId(prefix: string) {
    if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
      return `${prefix}-${crypto.randomUUID()}`;
    }
    return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
  }

  function addNode(type: 'postgres' | 'mysql' | 'mongodb' | 'cassandra' | 'schema_map' | 'join' | 'filter' | 'select' | 'rename' | 'cast' | 'derived' | 'parquet') {
    const id = newId(type);
    const position = nextPosition();

    if (type === 'postgres') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: {
            config: {
              host: 'postgresql.crossql.orb.local',
              port: 5432,
              user: 'postgres',
              password: 'postgres',
              database: 'crossql',
              query: 'SELECT 1 as id'
            }
          },
          position
        }
      ];
      return;
    }

    if (type === 'mysql') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: {
            config: {
              host: 'mysql.crossql.orb.local',
              port: 3306,
              user: 'crossql',
              password: 'crossql',
              database: 'crossql',
              query: 'SELECT 1 as id'
            }
          },
          position
        }
      ];
      return;
    }

    if (type === 'mongodb') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: {
            config: {
              uri: 'mongodb://root:root@mongodb.crossql.orb.local:27017',
              database: '',
              collection: '',
              filter: '{}',
              projection: '{}',
              flatten_depth: 1
            }
          },
          position
        }
      ];
      return;
    }

    if (type === 'cassandra') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: {
            config: {
              contact_points: 'cassandra.crossql.orb.local:9042',
              keyspace: 'crossql',
              query: 'SELECT * FROM regions;'
            }
          },
          position
        }
      ];
      return;
    }

    if (type === 'schema_map') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: { config: { columns: [] } },
          position: { x: 360, y: position.y }
        }
      ];
      return;
    }

    if (type === 'join') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: { config: { how: 'inner', left_on: 'id', right_on: 'id' } },
          position: { x: 470, y: position.y }
        }
      ];
      return;
    }

    if (type === 'filter') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: { config: { column: '', op: 'eq', value_type: 'string', value: '' } },
          position: { x: 360, y: position.y }
        }
      ];
      return;
    }

    if (type === 'select') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: { config: { columns: [] } },
          position: { x: 360, y: position.y }
        }
      ];
      return;
    }

    if (type === 'rename') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: { config: { mappings: [] } },
          position: { x: 360, y: position.y }
        }
      ];
      return;
    }

    if (type === 'cast') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: { config: { casts: [] } },
          position: { x: 360, y: position.y }
        }
      ];
      return;
    }

    if (type === 'derived') {
      nodes = [
        ...nodes,
        {
          id,
          type,
          data: { config: { name: 'derived', op: 'upper', left: '', right_kind: 'column', right: '' } },
          position: { x: 360, y: position.y }
        }
      ];
      return;
    }

    nodes = [
      ...nodes,
      {
        id,
        type,
        data: { config: { path: `/tmp/${id}.parquet`, compression: 'snappy', row_group_size: 524288 } },
        position: { x: 650, y: position.y }
      }
    ];
  }

  let nodes = $state<any[]>([
    {
      id: 'out1',
      type: 'parquet',
      data: { config: { path: '/tmp/test.parquet', compression: 'snappy', row_group_size: 524288 } },
      position: { x: 600, y: 100 },
    }
  ]);

  let edges = $state<any[]>([]);

  async function loadSchema() {
    schemaState = 'idle';
    schemaError = null;
    schemaFields = [];

    if (!selectedNodeId) return;
    const node = nodes.find((n: any) => n.id === selectedNodeId);
    if (!node) return;
    if (node.type !== 'postgres' && node.type !== 'mysql' && node.type !== 'mongodb' && node.type !== 'cassandra' && node.type !== 'schema_map') return;

    schemaState = 'loading';
    try {
      schemaFields = await fetchSchemaForNode(selectedNodeId);
      schemaState = 'ready';
    } catch (e) {
      schemaState = 'error';
      schemaError = `${e}`;
    }
  }

  $effect(() => {
    void loadSchema();
  });

  async function loadPreview() {
    previewState = 'idle';
    previewError = null;
    previewColumns = [];
    previewRows = [];

    if (!selectedNodeId) return;
    const node = nodes.find((n: any) => n.id === selectedNodeId);
    if (!node) return;
    if (node.type !== 'join') return;

    previewState = 'loading';
    try {
      const result = await invoke('preview_pipeline_node', { nodes, edges, nodeId: selectedNodeId });
      const r = result as any;
      previewColumns = Array.isArray(r?.columns) ? r.columns : [];
      previewRows = Array.isArray(r?.rows) ? r.rows : [];
      previewState = 'ready';
    } catch (e) {
      previewState = 'error';
      previewError = `${e}`;
    }
  }

  $effect(() => {
    void loadPreview();
  });

  function formatBytes(bytes: number) {
    if (!Number.isFinite(bytes) || bytes < 0) return `${bytes} B`;
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let n = bytes;
    let u = 0;
    while (n >= 1024 && u < units.length - 1) {
      n /= 1024;
      u += 1;
    }
    return `${n.toFixed(u === 0 ? 0 : 2)} ${units[u]}`;
  }

  async function loadUsage() {
    usageState = 'loading';
    usageError = null;
    try {
      const result = await invoke('get_process_usage');
      const r = result as any;
      usageCpuPercent = typeof r?.cpu_percent === 'number' ? r.cpu_percent : null;
      usageMemoryBytes = typeof r?.memory_bytes === 'number' ? r.memory_bytes : null;
      usageState = 'ready';
    } catch (e) {
      usageState = 'error';
      usageError = `${e}`;
      usageCpuPercent = null;
      usageMemoryBytes = null;
    }
  }

  $effect(() => {
    void loadUsage();
    const id = setInterval(() => void loadUsage(), 5000);
    return () => clearInterval(id);
  });

  async function cancelRun() {
    if (!runId) return;
    invokeResult = 'Cancelling…';
    try {
      await invoke('cancel_pipeline_run', { runId });
      appendLog('Cancel requested');
    } catch (e: any) {
      runState = 'error';
      invokeResult = `Error: ${e?.toString?.() ?? e}`;
    }
  }

  async function waitForRun(id: string) {
    try {
      const result = await invoke('await_pipeline_run', { runId: id });
      if (runId !== id) return;
      const r = result as any;
      runState = 'success';
      if (typeof r?.row_count === 'number' && typeof r?.file_size_bytes === 'number' && typeof r?.path === 'string') {
        runRowCount = r.row_count;
        runFileSizeBytes = r.file_size_bytes;
        runOutputPath = r.path;
        invokeResult = `Success: ${r.row_count} rows → ${r.path} (${formatBytes(r.file_size_bytes)})`;
      } else {
        invokeResult = 'Success';
      }
      if (Array.isArray(r?.node_stats)) {
        applyNodeStats(r.node_stats);
      }
      setAllNodeRunState('done');
      appendLog('Run finished');
      runId = null;
    } catch (e: any) {
      if (runId !== id) return;
      runState = 'error';
      invokeResult = `Error: ${e?.toString?.() ?? e}`;
      setAllNodeRunState('error', e?.toString?.() ?? `${e}`);
      appendLog(`Error: ${e?.toString?.() ?? e}`);
      runId = null;
    }
  }

  async function testInvoke() {
    runState = 'running';
    runRowCount = null;
    runFileSizeBytes = null;
    runOutputPath = null;

    const outputNodes = nodes.filter((n: any) => n.type === 'parquet');
    if (outputNodes.length !== 1) {
      runState = 'error';
      invokeResult = "Error: Exactly one Parquet Output node is required";
      return;
    }

    const outputNode = outputNodes[0];
    if (!outputNode?.data?.config) {
      runState = 'error';
      invokeResult = "Error: Missing output node config";
      return;
    }

    const output = outputNode.data.config as any;

    for (const n of nodes) {
      if (n.type !== 'postgres' && n.type !== 'mysql' && n.type !== 'mongodb' && n.type !== 'cassandra' && n.type !== 'join') continue;
      if (!n?.data?.config) {
        runState = 'error';
        invokeResult = `Error: Missing config for node ${n.id}`;
        return;
      }

      const cfg = n.data.config as any;

      if (n.type === 'mongodb') {
        if (!cfg.uri) {
          runState = 'error';
          invokeResult = "Error: MongoDB 'uri' is required";
          return;
        }
        if (!cfg.database) {
          runState = 'error';
          invokeResult = "Error: MongoDB 'database' is required";
          return;
        }
        if (!cfg.collection) {
          runState = 'error';
          invokeResult = "Error: MongoDB 'collection' is required";
          return;
        }
      } else if (n.type === 'cassandra') {
        if (!cfg.contact_points) {
          runState = 'error';
          invokeResult = "Error: Cassandra 'contact_points' is required";
          return;
        }
        if (!cfg.keyspace) {
          runState = 'error';
          invokeResult = "Error: Cassandra 'keyspace' is required";
          return;
        }
        if (!cfg.query) {
          runState = 'error';
          invokeResult = "Error: Cassandra 'query' is required";
          return;
        }
      } else if (n.type === 'join') {
        if (!cfg.left_on || !cfg.right_on) {
          runState = 'error';
          invokeResult = "Error: Join requires left_on and right_on";
          return;
        }
      } else {
        if (!cfg.host) {
          runState = 'error';
          invokeResult = "Error: Source 'host' is required";
          return;
        }
        if (!cfg.port) {
          runState = 'error';
          invokeResult = "Error: Source 'port' is required";
          return;
        }
        if (!cfg.user) {
          runState = 'error';
          invokeResult = "Error: Source 'user' is required";
          return;
        }
        if (cfg.password === undefined || cfg.password === null) {
          runState = 'error';
          invokeResult = "Error: Source 'password' is required";
          return;
        }
        if (!cfg.database) {
          runState = 'error';
          invokeResult = "Error: Source 'database' is required";
          return;
        }
        if (!cfg.query) {
          runState = 'error';
          invokeResult = "Error: Source 'query' is required";
          return;
        }
      }
    }

    if (!output.path) {
      runState = 'error';
      invokeResult = "Error: Output path is required";
      return;
    }

    setAllNodeRunState('running');
    runLogs = [];
    appendLog('Starting run…');
    invokeResult = "Starting run...";
    try {
      const id = await invoke('start_pipeline_run', { nodes, edges });
      runId = typeof id === 'string' ? id : id ? `${id}` : null;
      if (runId) appendLog(`Run id: ${runId}`);
      invokeResult = "Running pipeline...";
      if (runId) void waitForRun(runId);
    } catch (e) {
      runState = 'error';
      invokeResult = `Error: ${e}`;
    }
  }
</script>

<div class="flex h-screen w-full bg-warm-bg overflow-hidden">
  <!-- Sidebar Palette -->
  <aside class="w-64 bg-warm-panel border-r border-warm-border p-4 flex flex-col">
    <h2 class="text-warm-text font-bold mb-4">Node Palette</h2>
    <div class="flex flex-col gap-4">
      <div>
        <div class="text-xs text-warm-sub font-semibold mb-2">Sources</div>
        <div class="flex flex-col gap-2">
          <button
            type="button"
            onclick={() => addNode('postgres')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>🐘</span><span>PostgreSQL</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('mysql')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>🐬</span><span>MySQL</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('mongodb')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>🍃</span><span>MongoDB</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('cassandra')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>👁</span><span>Cassandra</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
        </div>
      </div>

      <div>
        <div class="text-xs text-warm-sub font-semibold mb-2">Outputs</div>
        <div class="flex flex-col gap-2">
          <button
            type="button"
            onclick={() => addNode('parquet')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>📦</span><span>Parquet</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
        </div>
      </div>

      <div>
        <div class="text-xs text-warm-sub font-semibold mb-2">Transforms</div>
        <div class="flex flex-col gap-2">
          <button
            type="button"
            onclick={() => addNode('schema_map')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>🔀</span><span>Schema Map</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('join')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>🎲</span><span>Join</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('filter')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>🔽</span><span>Filter</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('select')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>✅</span><span>Select</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('rename')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>✏️</span><span>Rename</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('cast')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>🧬</span><span>Cast</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
          <button
            type="button"
            onclick={() => addNode('derived')}
            class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
          >
            <span class="flex items-center gap-2"><span>➕</span><span>Derived</span></span>
            <span class="text-warm-muted text-xs">Add</span>
          </button>
        </div>
      </div>
    </div>
    
    <div class="mt-auto border-t border-warm-border pt-4">
      <button 
        onclick={testInvoke}
        class="w-full px-4 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm"
        disabled={runState === 'running'}
      >
        Run Pipeline
      </button>
      <button
        type="button"
        onclick={cancelRun}
        class="w-full mt-2 px-4 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm"
        disabled={!runId}
      >
        Cancel
      </button>
      <div class="text-xs mt-2">
        {#if runState === 'running'}
          <div class="text-warm-sub">Running…</div>
        {:else if runState === 'success'}
          <div class="text-[#4A7C59]">{invokeResult}</div>
        {:else if runState === 'error'}
          <div class="text-[#B85C4A]">{invokeResult}</div>
        {:else}
          <div class="text-warm-muted">{invokeResult}</div>
        {/if}
      </div>
      <div class="mt-3 border border-warm-border rounded bg-warm-bg">
        <div class="px-3 py-2 text-xs font-semibold text-warm-sub border-b border-warm-border">
          Execution Log
        </div>
        <div class="max-h-44 overflow-auto px-3 py-2">
          {#if runLogs.length === 0}
            <div class="text-xs text-warm-muted">No logs yet</div>
          {:else}
            <div class="flex flex-col gap-1">
              {#each runLogs as l (l.ts)}
                <div class="text-[11px] text-warm-sub">
                  {new Date(l.ts).toLocaleTimeString()} — {l.message}
                </div>
              {/each}
            </div>
          {/if}
        </div>
      </div>
    </div>
  </aside>

  <!-- Main Canvas Area -->
  <main class="flex-1 bg-warm-canvas relative">
    <PipelineCanvas bind:nodes bind:edges bind:selectedNodeId />
  </main>

  <!-- Config Panel -->
  <aside class="w-80 bg-warm-panel border-l border-warm-border p-4 flex flex-col">
    <h2 class="text-warm-text font-bold mb-4">Configuration</h2>
    <div class="flex-1 overflow-auto">
      {#if selectedNodeId}
        {@const selectedNode = nodes.find((n: any) => n.id === selectedNodeId)}
        {#if selectedNode}
          <div class="text-sm text-warm-text font-medium mb-1">
            {selectedNode.type} ({selectedNode.id})
          </div>
          {#if selectedNode.type === 'postgres' || selectedNode.type === 'mysql' || selectedNode.type === 'mongodb' || selectedNode.type === 'cassandra' || selectedNode.type === 'schema_map'}
            <div class="text-xs text-warm-sub mb-2">
              {selectedNode.type === 'schema_map' ? 'Schema (after map)' : 'Schema'}
            </div>
            {#if schemaState === 'loading'}
              <div class="text-xs text-warm-muted">Loading…</div>
            {:else if schemaState === 'error'}
              <div class="text-xs text-[#B85C4A]">{schemaError}</div>
            {:else if schemaState === 'ready'}
              {#if schemaFields.length === 0}
                <div class="text-xs text-warm-muted">No columns</div>
              {:else}
                <div class="flex flex-col gap-2">
                  {#each schemaFields as f (f.name)}
                    <div class="flex items-center justify-between gap-3">
                      <div class="text-xs text-warm-text truncate">{f.name}</div>
                      <TypeBadge dtype={f.dtype} />
                    </div>
                  {/each}
                </div>
              {/if}
            {:else}
              <div class="text-xs text-warm-muted">Select a source node to preview schema</div>
            {/if}
          {:else if selectedNode.type === 'join'}
            <div class="text-xs text-warm-sub mb-2">Preview (first 50 rows)</div>
            {#if previewState === 'loading'}
              <div class="text-xs text-warm-muted">Loading…</div>
            {:else if previewState === 'error'}
              <div class="text-xs text-[#B85C4A]">{previewError}</div>
            {:else if previewState === 'ready'}
              {#if previewColumns.length === 0}
                <div class="text-xs text-warm-muted">No columns</div>
              {:else}
                <div class="border border-warm-border rounded overflow-auto">
                  <table class="w-full text-xs">
                    <thead class="bg-warm-bg sticky top-0">
                      <tr>
                        {#each previewColumns as c (c)}
                          <th class="text-left px-2 py-1 border-b border-warm-border font-semibold text-warm-sub whitespace-nowrap">{c}</th>
                        {/each}
                      </tr>
                    </thead>
                    <tbody>
                      {#each previewRows as r, i (i)}
                        <tr class="odd:bg-white even:bg-warm-panel/40">
                          {#each previewColumns as c (c)}
                            <td class="px-2 py-1 border-b border-warm-border text-warm-text whitespace-nowrap">
                              {r?.[c] === null || r?.[c] === undefined ? '—' : `${r[c]}`}
                            </td>
                          {/each}
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              {/if}
            {:else}
              <div class="text-xs text-warm-muted">Select a Join node to preview</div>
            {/if}
          {:else}
            <div class="text-xs text-warm-muted">No schema preview for this node type</div>
          {/if}
        {:else}
          <div class="text-warm-muted text-sm italic">Select a node</div>
        {/if}
      {:else}
        <div class="text-warm-muted text-sm italic">
          Select a node to see its schema
        </div>
      {/if}
    </div>

    <div class="mt-4 border-t border-warm-border pt-3">
      <div class="text-xs text-warm-sub font-semibold mb-2">App Usage</div>
      {#if usageState === 'loading'}
        <div class="text-xs text-warm-muted">Loading…</div>
      {:else if usageState === 'error'}
        <div class="text-xs text-[#B85C4A] truncate">{usageError}</div>
      {:else if usageState === 'ready'}
        <div class="flex flex-col gap-1 text-xs text-warm-text">
          <div class="flex items-center justify-between">
            <div class="text-warm-sub">CPU</div>
            <div>{usageCpuPercent === null ? '—' : `${usageCpuPercent.toFixed(1)}%`}</div>
          </div>
          <div class="flex items-center justify-between">
            <div class="text-warm-sub">Memory</div>
            <div>{usageMemoryBytes === null ? '—' : formatBytes(usageMemoryBytes)}</div>
          </div>
          <div class="text-[11px] text-warm-muted">Refreshes every 5s</div>
        </div>
      {:else}
        <div class="text-xs text-warm-muted">Idle</div>
      {/if}
    </div>
  </aside>
</div>
