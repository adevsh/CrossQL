<script lang="ts">
  import { SvelteFlow, Background, Controls, MiniMap } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import PostgresSourceNode from './nodes/PostgresSourceNode.svelte';
  import OutputNode from './nodes/OutputNode.svelte';

  let { nodes = $bindable(), edges = $bindable() } = $props();

  const nodeTypes = {
    postgres: PostgresSourceNode,
    parquet: OutputNode
  };
</script>

<div class="h-full w-full bg-warm-canvas">
  <SvelteFlow 
    bind:nodes 
    bind:edges 
    {nodeTypes}
    fitView
    class="bg-warm-canvas"
  >
    <Background patternColor="#D9D0C0" gap={20} size={1} />
    <Controls showLock={false} /> 
    <MiniMap nodeColor="#C07A3A" maskColor="#F5F0E8" />
  </SvelteFlow>
</div>
