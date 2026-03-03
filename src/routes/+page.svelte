<script lang="ts">
  import PipelineCanvas from '$lib/components/PipelineCanvas.svelte';
  import { invoke } from "@tauri-apps/api/core";

  let invokeResult = $state("Idle");

  let runState = $state<'idle' | 'running' | 'success' | 'error'>('idle');
  let runRowCount = $state<number | null>(null);
  let runFileSizeBytes = $state<number | null>(null);
  let runOutputPath = $state<string | null>(null);

  let nodes = $state([
    {
      id: 'src1',
      type: 'postgres',
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
      position: { x: 100, y: 100 },
    },
    {
      id: 'out1',
      type: 'parquet',
      data: { config: { path: '/tmp/test.parquet', compression: 'snappy', row_group_size: 524288 } },
      position: { x: 600, y: 100 },
    }
  ]);

  let edges = $state([
    { id: 'e1', source: 'src1', target: 'out1' }
  ]);

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

    if (sourceNode.type !== 'postgres') {
      runState = 'error';
      invokeResult = "Error: Output must be connected from a PostgreSQL Source node";
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

    if (!source.host) {
      runState = 'error';
      invokeResult = "Error: Postgres 'host' is required";
      return;
    }
    if (!source.port) {
      runState = 'error';
      invokeResult = "Error: Postgres 'port' is required";
      return;
    }
    if (!source.user) {
      runState = 'error';
      invokeResult = "Error: Postgres 'user' is required";
      return;
    }
    if (!source.password) {
      runState = 'error';
      invokeResult = "Error: Postgres 'password' is required";
      return;
    }
    if (!source.database) {
      runState = 'error';
      invokeResult = "Error: Postgres 'database' is required";
      return;
    }
    if (!source.query) {
      runState = 'error';
      invokeResult = "Error: Postgres 'query' is required";
      return;
    }

    if (!output.path) {
      runState = 'error';
      invokeResult = "Error: Output path is required";
      return;
    }

    invokeResult = "Running pipeline...";
    try {
      const result = await invoke("run_postgres_to_parquet", { source, output });
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
    <div class="text-warm-muted text-sm italic">
      Draggable nodes will go here (Phase 1)
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
    <PipelineCanvas bind:nodes bind:edges />
  </main>

  <!-- Config Panel -->
  <aside class="w-80 bg-warm-panel border-l border-warm-border p-4">
    <h2 class="text-warm-text font-bold mb-4">Configuration</h2>
    <div class="text-warm-muted text-sm italic">
      Node properties will appear here on selection
    </div>
  </aside>
</div>
