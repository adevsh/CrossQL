<script lang="ts">
  import PipelineCanvas from '$lib/components/PipelineCanvas.svelte';
  import TypeBadge from '$lib/components/TypeBadge.svelte';
  import { invoke } from "@tauri-apps/api/core";

  let invokeResult = $state("Idle");

  let runState = $state<'idle' | 'running' | 'success' | 'error'>('idle');
  let runRowCount = $state<number | null>(null);
  let runFileSizeBytes = $state<number | null>(null);
  let runOutputPath = $state<string | null>(null);

  let selectedNodeId = $state<string | null>(null);
  let schemaState = $state<'idle' | 'loading' | 'ready' | 'error'>('idle');
  let schemaError = $state<string | null>(null);
  let schemaFields = $state<Array<{ name: string; dtype: string }>>([]);

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

  function addNode(type: 'postgres' | 'mysql' | 'mongodb' | 'cassandra' | 'parquet') {
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

  let edges = $state<any[]>([
    { id: 'e1', source: 'src1', target: 'out1' }
  ]);

  async function loadSchema() {
    schemaState = 'idle';
    schemaError = null;
    schemaFields = [];

    if (!selectedNodeId) return;
    const node = nodes.find((n: any) => n.id === selectedNodeId);
    if (!node) return;
    if (node.type !== 'postgres' && node.type !== 'mysql' && node.type !== 'mongodb' && node.type !== 'cassandra') return;
    if (!node.data?.config) return;

    schemaState = 'loading';
    try {
      const command =
        node.type === 'postgres'
          ? 'preview_postgres_schema'
          : node.type === 'mysql'
            ? 'preview_mysql_schema'
            : node.type === 'mongodb'
              ? 'preview_mongodb_schema'
              : 'preview_cassandra_schema';
      const result = await invoke(command, { source: node.data.config });
      schemaFields = (result as any[])?.map((x: any) => ({ name: x.name, dtype: x.dtype })) ?? [];
      schemaState = 'ready';
    } catch (e) {
      schemaState = 'error';
      schemaError = `${e}`;
    }
  }

  $effect(() => {
    void loadSchema();
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
    const incomingEdges = edges.filter((e: any) => e.target === outputNode.id);
    if (incomingEdges.length !== 1) {
      runState = 'error';
      invokeResult = "Error: Output node must have exactly one incoming edge";
      return;
    }

    const sourceNodeId = incomingEdges[0].source;
    const sourceNode = nodes.find((n: any) => n.id === sourceNodeId);
    if (!sourceNode) {
      runState = 'error';
      invokeResult = "Error: Output edge source node not found";
      return;
    }

    if (sourceNode.type !== 'postgres' && sourceNode.type !== 'mysql' && sourceNode.type !== 'mongodb' && sourceNode.type !== 'cassandra') {
      runState = 'error';
      invokeResult = "Error: Output must be connected from a source node";
      return;
    }

    if (!sourceNode?.data?.config) {
      runState = 'error';
      invokeResult = "Error: Missing Postgres node config";
      return;
    }

    if (!outputNode?.data?.config) {
      runState = 'error';
      invokeResult = "Error: Missing output node config";
      return;
    }

    const source = sourceNode.data.config as any;
    const output = outputNode.data.config as any;

    if (sourceNode.type === 'mongodb') {
      if (!source.uri) {
        runState = 'error';
        invokeResult = "Error: MongoDB 'uri' is required";
        return;
      }
      if (!source.database) {
        runState = 'error';
        invokeResult = "Error: MongoDB 'database' is required";
        return;
      }
      if (!source.collection) {
        runState = 'error';
        invokeResult = "Error: MongoDB 'collection' is required";
        return;
      }
    } else if (sourceNode.type === 'cassandra') {
      if (!source.contact_points) {
        runState = 'error';
        invokeResult = "Error: Cassandra 'contact_points' is required";
        return;
      }
      if (!source.keyspace) {
        runState = 'error';
        invokeResult = "Error: Cassandra 'keyspace' is required";
        return;
      }
      if (!source.query) {
        runState = 'error';
        invokeResult = "Error: Cassandra 'query' is required";
        return;
      }
    } else {
      if (!source.host) {
        runState = 'error';
        invokeResult = "Error: Source 'host' is required";
        return;
      }
      if (!source.port) {
        runState = 'error';
        invokeResult = "Error: Source 'port' is required";
        return;
      }
      if (!source.user) {
        runState = 'error';
        invokeResult = "Error: Source 'user' is required";
        return;
      }
      if (source.password === undefined || source.password === null) {
        runState = 'error';
        invokeResult = "Error: Source 'password' is required";
        return;
      }
      if (!source.database) {
        runState = 'error';
        invokeResult = "Error: Source 'database' is required";
        return;
      }
      if (!source.query) {
        runState = 'error';
        invokeResult = "Error: Source 'query' is required";
        return;
      }
    }

    if (!output.path) {
      runState = 'error';
      invokeResult = "Error: Output path is required";
      return;
    }

    invokeResult = "Running pipeline...";
    try {
      const command =
        sourceNode.type === 'postgres'
          ? 'run_postgres_to_parquet'
          : sourceNode.type === 'mysql'
            ? 'run_mysql_to_parquet'
            : sourceNode.type === 'mongodb'
              ? 'run_mongodb_to_parquet'
              : 'run_cassandra_to_parquet';
      const result = await invoke(command, { source, output });
      const r = result as any;
      runRowCount = typeof r?.row_count === 'number' ? r.row_count : null;
      runFileSizeBytes = typeof r?.file_size_bytes === 'number' ? r.file_size_bytes : null;
      runOutputPath = typeof r?.path === 'string' ? r.path : null;
      runState = 'success';
      if (runRowCount !== null && runFileSizeBytes !== null && runOutputPath) {
        invokeResult = `Success: ${runRowCount} rows → ${runOutputPath} (${formatBytes(runFileSizeBytes)})`;
      } else {
        invokeResult = "Success";
      }
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
    </div>
    
    <div class="mt-auto border-t border-warm-border pt-4">
      <button 
        onclick={testInvoke}
        class="w-full px-4 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm"
      >
        Run Pipeline
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
    </div>
  </aside>

  <!-- Main Canvas Area -->
  <main class="flex-1 bg-warm-canvas relative">
    <PipelineCanvas bind:nodes bind:edges bind:selectedNodeId />
  </main>

  <!-- Config Panel -->
  <aside class="w-80 bg-warm-panel border-l border-warm-border p-4">
    <h2 class="text-warm-text font-bold mb-4">Configuration</h2>
    {#if selectedNodeId}
      {@const selectedNode = nodes.find((n: any) => n.id === selectedNodeId)}
      {#if selectedNode}
        <div class="text-sm text-warm-text font-medium mb-1">
          {selectedNode.type} ({selectedNode.id})
        </div>
        {#if selectedNode.type === 'postgres' || selectedNode.type === 'mysql' || selectedNode.type === 'mongodb' || selectedNode.type === 'cassandra'}
          <div class="text-xs text-warm-sub mb-2">Schema</div>
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
  </aside>
</div>
