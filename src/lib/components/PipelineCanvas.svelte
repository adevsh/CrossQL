<script lang="ts">
  import { SvelteFlow, Background, Controls, MiniMap } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import PostgresSourceNode from './nodes/PostgresSourceNode.svelte';
  import MySQLSourceNode from './nodes/MySQLSourceNode.svelte';
  import MongoSourceNode from './nodes/MongoSourceNode.svelte';
  import CassandraSourceNode from './nodes/CassandraSourceNode.svelte';
  import SchemaMapNode from './nodes/SchemaMapNode.svelte';
  import JoinNode from './nodes/JoinNode.svelte';
  import FilterNode from './nodes/FilterNode.svelte';
  import SelectNode from './nodes/SelectNode.svelte';
  import RenameNode from './nodes/RenameNode.svelte';
  import CastNode from './nodes/CastNode.svelte';
  import DerivedColumnNode from './nodes/DerivedColumnNode.svelte';
  import OutputNode from './nodes/OutputNode.svelte';

  let { nodes = $bindable(), edges = $bindable(), selectedNodeId = $bindable<string | null>(null) } = $props();

  const nodeTypes = {
    postgres: PostgresSourceNode,
    mysql: MySQLSourceNode,
    mongodb: MongoSourceNode,
    cassandra: CassandraSourceNode,
    schema_map: SchemaMapNode,
    join: JoinNode,
    filter: FilterNode,
    select: SelectNode,
    rename: RenameNode,
    cast: CastNode,
    derived: DerivedColumnNode,
    parquet: OutputNode
  };

  function onSelectionChange({ nodes }: { nodes: any[]; edges: any[] }) {
    selectedNodeId = nodes[0]?.id ?? null;
  }
</script>

<div class="h-full w-full bg-warm-canvas">
  <SvelteFlow 
    bind:nodes 
    bind:edges 
    {nodeTypes}
    onselectionchange={onSelectionChange}
    fitView
    fitViewOptions={{ padding: 0.6, maxZoom: 0.85 }}
    class="bg-warm-canvas"
  >
    <Background patternColor="#D9D0C0" gap={20} size={1} />
    <Controls showLock={false} /> 
    <MiniMap nodeColor="#C07A3A" maskColor="#F5F0E8" />
  </SvelteFlow>
</div>
